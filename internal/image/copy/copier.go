package copy

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/containerd/containerd/platforms"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/ahamlinman/magic-mirror/internal/image"
	"github.com/ahamlinman/magic-mirror/internal/log"
	"github.com/ahamlinman/magic-mirror/internal/work"
)

// CopyAll performs a bulk copy between OCI image registries based on the
// provided copy specs, using the provided concurrency for each component of the
// overall operation.
func CopyAll(concurrency int, specs ...Spec) error {
	keys, err := coalesceRequests(specs)
	if err != nil {
		return err
	}
	copier := newCopier(concurrency)
	return copier.CopyAll(keys...)
}

type copier struct {
	queue *work.Queue[Spec, work.NoValue]

	blobs        *blobCopier
	srcManifests *manifestCache
	platforms    *platformCopier
	dstManifests *manifestCache
	dstIndexer   *blobIndexer

	statsTimer *time.Timer
}

func newCopier(concurrency int) *copier {
	blobs := newBlobCopier(concurrency)
	srcManifests := newManifestCache(concurrency)
	platforms := newPlatformCopier(srcManifests, blobs)
	dstManifests := newManifestCache(concurrency)
	dstIndexer := newBlobIndexer(concurrency, blobs)

	c := &copier{
		blobs:        blobs,
		srcManifests: srcManifests,
		platforms:    platforms,
		dstManifests: dstManifests,
		dstIndexer:   dstIndexer,
	}
	c.queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	c.statsTimer = time.AfterFunc(statsInterval, c.printStats)
	return c
}

func (c *copier) CopyAll(specs ...Spec) error {
	_, err := c.queue.GetAll(specs...)
	c.printStats()
	return err
}

const statsInterval = 5 * time.Second

func (c *copier) printStats() {
	var (
		blobsDone, blobsTotal         = c.blobs.Stats()
		platformsDone, platformsTotal = c.platforms.Stats()
		imagesDone, imagesTotal       = c.queue.Stats()
	)
	log.Printf(
		"[stats] blobs: %d of %d copied; platforms: %d of %d copied; images: %d of %d done",
		blobsDone, blobsTotal,
		platformsDone, platformsTotal,
		imagesDone, imagesTotal,
	)
	c.statsTimer.Reset(statsInterval)
}

func (c *copier) handleRequest(_ *work.QueueHandle, spec Spec) error {
	log.Verbosef("[image]\tstarting copy from %s to %s", spec.Src, spec.Dst)

	var (
		dstWait     sync.WaitGroup
		dstManifest image.ManifestKind
		dstErr      error
	)
	dstWait.Add(1)
	go func() {
		defer dstWait.Done()
		dstManifest, dstErr = c.dstManifests.Get(spec.Dst)
	}()

	srcManifest, err := c.srcManifests.Get(spec.Src)
	if err != nil {
		return err
	}

	dstWait.Wait()
	if dstErr == nil {
		c.dstIndexer.Submit(spec.Dst.Repository, dstManifest)
		if bytes.Equal(srcManifest.Encoded(), dstManifest.Encoded()) && (spec.Transform == Transform{}) {
			log.Verbosef("[image]\tno change from %s to %s", spec.Src, spec.Dst)
			return nil
		}
	}

	srcMediaType := srcManifest.GetMediaType()
	switch {
	case srcMediaType.IsIndex():
		err = c.copyIndex(spec, srcManifest.(image.Index))
	case srcMediaType.IsManifest():
		_, err = c.platforms.Copy(spec.Src, spec.Dst)
	default:
		err = fmt.Errorf("unknown manifest type for %s: %s", spec.Src, srcMediaType)
	}
	if err != nil {
		return err
	}

	log.Verbosef("[image]\tfully mirrored %s to %s", spec.Src, spec.Dst)
	return nil
}

func (c *copier) copyIndex(spec Spec, srcIndex image.Index) error {
	src := spec.Src
	dst := spec.Dst

	if err := srcIndex.Validate(); err != nil {
		return err
	}

	var (
		uploadIndex    = srcIndex
		dstIndex       image.ParsedIndex
		dstIndexCopied bool
	)
	ensureNewDstIndex := func() {
		if !dstIndexCopied {
			dstIndex = image.DeepCopy(srcIndex).(image.Index).Parsed()
			dstIndexCopied = true
		}
	}

	selectedDescriptors := srcIndex.Parsed().Manifests
	limitPlatforms := spec.Transform.LimitPlatforms.ToPlatforms()
	if len(limitPlatforms) > 0 {
		ensureNewDstIndex()
		selectedDescriptors = []v1.Descriptor{}
		matcher := platforms.Any(limitPlatforms...)
		for _, descriptor := range dstIndex.Manifests {
			if matcher.Match(*descriptor.Platform) {
				selectedDescriptors = append(selectedDescriptors, descriptor)
			}
		}
		dstIndex.Manifests = selectedDescriptors
	}

	imgsToCopy := make([]image.Image, len(selectedDescriptors))
	for i, descriptor := range selectedDescriptors {
		imgsToCopy[i] = image.Image{
			Repository: src.Repository,
			Digest:     descriptor.Digest,
		}
	}

	if len(imgsToCopy) == 0 {
		return fmt.Errorf("could not find any requested platforms in %s", src)
	}
	if len(imgsToCopy) == 1 {
		_, err := c.platforms.Copy(imgsToCopy[0], dst)
		return err
	}

	dstManifests, err := c.platforms.CopyAll(dst.Repository, imgsToCopy...)
	if err != nil {
		return err
	}
	for i, dstManifest := range dstManifests {
		desc := dstManifest.Descriptor()
		if desc.Digest != selectedDescriptors[i].Digest {
			ensureNewDstIndex()
			dstIndex.Manifests[i] = desc
			dstIndex.Manifests[i].Annotations = selectedDescriptors[i].Annotations
			dstIndex.Manifests[i].Platform = selectedDescriptors[i].Platform
		}
	}

	if dstIndexCopied {
		uploadIndex = dstIndex
	}
	return uploadManifest(dst, uploadIndex)
}
