# Deploying Pipelines

Log into the pipeline, and if necessary, update fly. (fly will warn you if you need to do the
`fly sync`)
```bash
fly login -c https://concourse.apachegeode-ci.info -t geode -n staging
fly -t geode sync
```
Now deploy the pipeline for your fork
```bash
./deploy_meta.sh <github account name>
```
In the browser, go to `https://concourse.apachegeode-ci.info/teams/staging/pipelines`, 
and select the meta pipleine `meta-<github account name>-<branch name>`


1) Meta pipeline will build its base docker image first, `build-meta-mini-docker-image`.
2) When complete start `set-images-pipeline`. When complete refresh browser to see new images pipeline.
3) Start `fork-branch-images-pipeline` and wait for all images to complete. (30m)
4) Back on the `meta-<...>` pipeline, start `set-pipeline` and wait for main pipeline to deploy. Refresh your browser.




# Generating Pipeline Templates Manually
To generate a pipeline, using jinja:
* Be in the pipelines directory or a subdirectory
* With ${GEODE_BRANCH} and ${GEODE_FORK} correctly set in your environment
(for the pipeline you want to create):

```bash
./render.py <path to jinja template> <path to jinja variables> <path to generated file>
```

The generated file should be named `generated-pipeline.yml`.