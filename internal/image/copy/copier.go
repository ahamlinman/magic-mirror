package copy

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type Request struct {
	From image.Image
	To   image.Image
}

func ValidateRequests(reqs ...Request) error {
	_, err := coalesceRequests(reqs)
	return err
}

func coalesceRequests(reqs []Request) ([]Request, error) {
	var errs []error

	sources := mapset.NewThreadUnsafeSet[image.Image]()
	for _, req := range reqs {
		sources.Add(req.From)
	}
	for _, req := range reqs {
		if sources.Contains(req.To) {
			errs = append(errs, fmt.Errorf("%s is both a source and a destination", req.To))
		}
	}

	coalesced := make([]Request, 0, len(reqs))
	requestsByDestination := make(map[image.Image]Request)
	for _, current := range reqs {
		previous, ok := requestsByDestination[current.To]
		if !ok {
			coalesced = append(coalesced, current)
			requestsByDestination[current.To] = current
			continue
		}
		if previous != current {
			errs = append(errs, fmt.Errorf("%s requests inconsistent copies from %s and %s", current.To, current.From, previous.From))
		}
	}

	return coalesced, errors.Join(errs...)
}

type Copier struct {
	*work.Queue[Request, work.NoValue]

	blobs           *blobCopier
	sourceManifests *manifestCache
	platforms       *platformCopier
	destManifests   *manifestCache
	destTracer      *destinationTracer
}

func NewCopier(workers int) *Copier {
	blobs := newBlobCopier(workers)
	sourceManifests := newManifestCache(workers)
	platforms := newPlatformCopier(sourceManifests, blobs)
	destManifests := newManifestCache(workers)
	destTracer := newDestinationTracer(destManifests, blobs)

	c := &Copier{
		blobs:           blobs,
		sourceManifests: sourceManifests,
		platforms:       platforms,
		destManifests:   destManifests,
		destTracer:      destTracer,
	}
	c.Queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *Copier) Copy(from, to image.Image) error {
	_, err := c.Queue.GetOrSubmit(Request{From: from, To: to}).Wait()
	return err
}

func (c *Copier) CopyAll(reqs ...Request) error {
	reqs, err := coalesceRequests(reqs)
	if err != nil {
		return err
	}
	_, err = c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *Copier) CloseSubmit() {
	// TODO: This is only safe after all Copier tasks are finished.
	c.Queue.CloseSubmit()
	c.platforms.CloseSubmit()
	c.sourceManifests.CloseSubmit()
	// TODO: Since we don't block on destination tracing, these may not be safe to
	// clean up. Need to figure out a cancellation strategy.
	// c.destTracer.CloseSubmit()
	// c.destManifests.CloseSubmit()
	c.blobs.CloseSubmit()
}

func (c *Copier) handleRequest(req Request) error {
	log.Printf("[image]\tstarting copy from %s to %s", req.From, req.To)

	c.destTracer.QueueTrace(req.To)

	manifest, err := c.sourceManifests.Get(req.From)
	if err != nil {
		return err
	}

	var parsedManifest struct {
		Manifests []struct {
			Digest image.Digest `json:"digest"`
		} `json:"manifests"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifest.Body), &parsedManifest); err != nil {
		return err
	}

	if len(parsedManifest.Manifests) == 0 {
		err = c.platforms.Copy(req.From, req.To.Repository)
	} else {
		imgs := make([]image.Image, len(parsedManifest.Manifests))
		for i, m := range parsedManifest.Manifests {
			imgs[i] = image.Image{Repository: req.From.Repository, Digest: m.Digest}
		}
		err = c.platforms.CopyAll(req.To.Repository, imgs...)
	}
	if err != nil {
		return err
	}

	if len(parsedManifest.Manifests) > 0 {
		if err := uploadManifest(req.To, manifest); err != nil {
			return err
		}
	}
	log.Printf("[image]\tfully copied %s to %s", req.From, req.To)
	return nil
}
