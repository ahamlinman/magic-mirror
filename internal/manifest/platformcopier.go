package manifest

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/blob"
	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type PlatformCopier struct {
	engine             *work.Queue[PlatformRequest, work.NoValue]
	manifestDownloader *Downloader
	blobCopier         *blob.Copier
}

type PlatformRequest struct {
	From image.Image
	To   image.Repository
}

func NewPlatformCopier(workers int, manifestDownloader *Downloader, blobCopier *blob.Copier) *PlatformCopier {
	c := &PlatformCopier{
		manifestDownloader: manifestDownloader,
		blobCopier:         blobCopier,
	}
	c.engine = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *PlatformCopier) RequestCopy(from image.Image, to image.Repository) PlatformCopyTask {
	return PlatformCopyTask{c.engine.GetOrSubmit(PlatformRequest{
		From: from,
		To:   to,
	})}
}

type PlatformCopyTask struct {
	*work.Task[work.NoValue]
}

func (t PlatformCopyTask) Wait() error {
	_, err := t.Task.Wait()
	return err
}

func (c *PlatformCopier) Close() {
	c.engine.CloseSubmit()
}

func (c *PlatformCopier) handleRequest(req PlatformRequest) error {
	manifestResponse, err := c.manifestDownloader.RequestDownload(req.From).Wait()
	if err != nil {
		return err
	}

	var manifest struct {
		Config struct {
			Digest image.Digest `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifestResponse.Body), &manifest); err != nil {
		return err
	}

	copyRequests := make([]blob.CopyRequest, len(manifest.Layers)+1)
	for i, layer := range manifest.Layers {
		c.blobCopier.RegisterSource(layer.Digest, req.From.Repository)
		copyRequests[i] = blob.CopyRequest{Digest: layer.Digest, To: req.To}
	}
	c.blobCopier.RegisterSource(manifest.Config.Digest, req.From.Repository)
	copyRequests[len(copyRequests)-1] = blob.CopyRequest{Digest: manifest.Config.Digest, To: req.To}

	_, err = c.blobCopier.GetOrSubmitAll(copyRequests...).WaitAll()
	if err != nil {
		return err
	}

	destImg := image.Image{Repository: req.To, Tag: req.From.Tag, Digest: req.From.Digest}
	err = uploadManifest(destImg, manifestResponse.ContentType, manifestResponse.Body)
	if err != nil {
		return err
	}

	log.Printf("[platform]\tcopied %s to %s", req.From, destImg.Repository)
	return nil
}
