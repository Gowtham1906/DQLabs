#!/bin/sh
cd temp
rm -rf Dqlabs-Client-2.0
git clone https://${GITHUB_TOKEN}@github.com/DQLabs-Inc/DQLabs-Client-2.0.git
cd DQLabs-Client-2.0
cp .env.dev .env.local
npm install --legacy-peer-deps
npm run build
aws s3 rm s3://frontendassets/static/ --recursive
aws s3 cp build s3://frontendassets/ --recursive
cd ../
rm -rf DQlabs-Client-2.0