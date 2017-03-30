# age-friendly-communities
Analyze San Diego's assisted living industry

## Data Packages

Data packages created specifically for this project are in the ``packages`` directory. 

After updating a package, wrangers should check the repo back into Github, then update the package in S3 and CKAN. *Be sure to update the package version number*

There are two steps for releasing a package: 

1) Build the Excel, ZIP and Filesystem packages
2) Sync the packages to CKAN


```bash

$ metasync -e -c -z -s s3://library.metatab.org
$ metakan --ckan http://data.sandiegodata.org --api <api_key>

```

You can find the CKAN Api key in your user profile on http://data.sandiegodata.org

If you want to test that packages are built correctly, you can just generate a Filesystem package:

```bash

$ metapack -f

```