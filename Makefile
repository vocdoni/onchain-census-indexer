IMAGE ?= onchain-census-indexer
CONTAINER ?= onchain-census-indexer

.PHONY: build run check-env

check-env:
	@if [ ! -f .env ]; then \
		echo "Missing .env file. Copy .env.example to .env and edit values."; \
		exit 1; \
	fi

build: check-env
	docker build -t $(IMAGE) .

run: check-env build
	docker run --rm \
		--name $(CONTAINER) \
		--env-file .env \
		-p 8080:8080 \
		-v "$(PWD)/data:/data" \
		$(IMAGE)
