package copy

import (
	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/log"
)

// blobIndexer discovers the existence of blobs in a repository using manifest
// information alone, and registers this information with a blobCopier.
//
// The goal of blob indexing is twofold: to identify when blobs already exist in
// a destination repository before we even make a HEAD request to the registry,
// and to identify potential cross-repository mount sources within each
// destination registry. Blob indexing is performed on a best-effort basis even
// when the image manifest at the destination is up to date.
type blobIndexer struct {
	manifests *manifestCache
	blobs     *blobCopier
}

func newBlobIndexer(concurrency int, blobs *blobCopier) *blobIndexer {
	return &blobIndexer{
		manifests: newManifestCache(concurrency),
		blobs:     blobs,
	}
}

// Submit begins the process of indexing the provided image.
func (bi *blobIndexer) Submit(repo image.Repository, manifest image.ManifestKind) {
	manifestType := manifest.GetMediaType()
	if manifestType.IsIndex() {
		bi.queueManifestsFromIndex(repo, manifest.(image.Index))
		return
	}
	if !manifestType.IsManifest() {
		return
	}

	parsed := manifest.(image.Manifest).Parsed()
	bi.blobs.RegisterSource(parsed.Config.Digest, repo)
	for _, layer := range parsed.Layers {
		bi.blobs.RegisterSource(layer.Digest, repo)
	}
	dgst := manifest.Descriptor().Digest
	log.Verbosef("[dstindex]\tindexed blobs referenced by %s@%s", repo, dgst)
}

func (bi *blobIndexer) queueManifestsFromIndex(repo image.Repository, index image.Index) {
	descriptors := index.Parsed().Manifests
	for _, desc := range descriptors {
		desc := desc
		go func() {
			manifest, err := bi.manifests.Get(image.Image{Repository: repo, Digest: desc.Digest})
			if err == nil {
				bi.Submit(repo, manifest)
			}
		}()
	}
}
