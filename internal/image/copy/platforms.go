package copy

import (
	"fmt"

	"github.com/opencontainers/go-digest"

	"github.com/ahamlinman/magic-mirror/internal/image"
	"github.com/ahamlinman/magic-mirror/internal/log"
	"github.com/ahamlinman/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, image.Manifest]

	manifests *manifestCache
	blobs     *blobCopier
}

type platformCopyRequest struct {
	Src image.Image
	Dst image.Image
}

func newPlatformCopier(manifests *manifestCache, blobs *blobCopier) *platformCopier {
	c := &platformCopier{
		manifests: manifests,
		blobs:     blobs,
	}
	c.Queue = work.NewQueue(0, c.handleRequest)
	return c
}

func (c *platformCopier) Copy(src image.Image, dst image.Image) (image.Manifest, error) {
	return c.Queue.Get(platformCopyRequest{Src: src, Dst: dst})
}

func (c *platformCopier) CopyAll(dst image.Repository, srcs ...image.Image) ([]image.Manifest, error) {
	reqs := make([]platformCopyRequest, len(srcs))
	for i, src := range srcs {
		reqs[i] = platformCopyRequest{
			Src: src,
			Dst: image.Image{
				Repository: dst,
				Digest:     src.Digest,
			},
		}
	}
	return c.Queue.GetAll(reqs...)
}

func (c *platformCopier) handleRequest(_ *work.QueueHandle, req platformCopyRequest) (m image.Manifest, err error) {
	srcManifest, err := c.manifests.Get(req.Src)
	if err != nil {
		return
	}
	if !srcManifest.GetMediaType().IsManifest() {
		err = fmt.Errorf("%s is a manifest list, but should be a manifest", req.Src)
		return
	}

	manifest := srcManifest.(image.Manifest)
	if err = manifest.Validate(); err != nil {
		return
	}

	layers := manifest.Parsed().Layers
	blobDigests := make([]digest.Digest, len(layers)+1)
	for i, layer := range layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = manifest.Parsed().Config.Digest
	if err = c.blobs.CopyAll(req.Src.Repository, req.Dst.Repository, blobDigests...); err != nil {
		return
	}

	dstImg := image.Image{
		Repository: req.Dst.Repository,
		Tag:        req.Dst.Tag,
		Digest:     manifest.Descriptor().Digest,
	}
	err = uploadManifest(dstImg, manifest)
	if err == nil {
		log.Verbosef("[platform]\tmirrored %s to %s", req.Src, dstImg)
	}
	return manifest, err
}
