package copy

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type Copier struct {
	*work.Queue[Request, work.NoValue]

	manifestDownloader *ManifestDownloader
	platformCopier     *PlatformCopier
}

type Request struct {
	From image.Image
	To   image.Image
}

func NewCopier(workers int, manifestDownloader *ManifestDownloader, platformCopier *PlatformCopier) *Copier {
	c := &Copier{
		manifestDownloader: manifestDownloader,
		platformCopier:     platformCopier,
	}
	c.Queue = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *Copier) Copy(from, to image.Image) error {
	_, err := c.Queue.GetOrSubmit(Request{From: from, To: to}).Wait()
	return err
}

func (c *Copier) CopyAll(reqs ...Request) error {
	_, err := c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *Copier) handleRequest(req Request) error {
	log.Printf("[image]\tstarting copy from %s to %s", req.From, req.To)

	manifestResponse, err := c.manifestDownloader.Get(req.From)
	if err != nil {
		return err
	}

	var manifest struct {
		Manifests []struct {
			Digest image.Digest `json:"digest"`
		} `json:"manifests"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifestResponse.Body), &manifest); err != nil {
		return err
	}

	if len(manifest.Manifests) == 0 {
		err = c.platformCopier.Copy(req.From, req.To.Repository)
	} else {
		imgs := make([]image.Image, len(manifest.Manifests))
		for i, m := range manifest.Manifests {
			imgs[i] = image.Image{Repository: req.From.Repository, Digest: string(m.Digest)}
		}
		err = c.platformCopier.CopyAll(req.To.Repository, imgs...)
	}
	if err != nil {
		return err
	}

	if len(manifest.Manifests) > 0 {
		if err := uploadManifest(req.To, manifestResponse.ContentType, manifestResponse.Body); err != nil {
			return err
		}
	}
	log.Printf("[image]\tfully copied %s to %s", req.From, req.To)
	return nil
}