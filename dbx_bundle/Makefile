DBX_BUNDLE = databricks bundle


deploy:
	${DBX_BUNDLE} deploy --target prd

deploy-dev:
	${DBX_BUNDLE} deploy

test:
	uv run pytest -vvv

run:
	${DBX_BUNDLE} run dbx_bundle_job --target prd --no-wait

run-dev:
	${DBX_BUNDLE} run dbx_bundle_job --no-wait

lint:
	uv run ruff check --fix .

format:
	uv run ruff format .

clean:
	for i in src/*.ipynb; do uv run jupyter nbconvert --clear-output $$i; done;
