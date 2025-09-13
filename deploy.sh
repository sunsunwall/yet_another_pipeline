#!/bin/bash
echo "Pushing to GitHub..."
git add .
git commit -m "Deploying latest changes"
git push origin main
echo "Checking build status..."
gcloud builds list --filter="source.repoSource.repoName=yet_another_pipeline AND status=WORKING OR status=SUCCESS OR status=FAILURE" --limit=1 --format="table(id,status,startTime,duration)"
echo "Build launched! Check GCP Console for details."