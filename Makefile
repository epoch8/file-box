DOCKER_REPOSITORY_IMAGE=test
VERSION=$(shell cat version)

build-arm:
	docker build . --platform=linux/amd64 -t $(DOCKER_REPOSITORY_IMAGE):$(VERSION) -f Dockerfile