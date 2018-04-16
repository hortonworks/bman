## Note

Please don't move these tests to Apache HDFS trunk. 

These are tests written to verify that protocol header works as expected. These are curl based scripts that will be replaced with Java based tests later

## Tests

1) full_life_cycle_test.sh -> creates a random bucket, will upload a zip file less than 10 MB, delete the local file, get the file back from Ozone and then delete file and bucket from Ozone. This is a full lifecyle test of a bucket and objects.

2) test_auth.sh -> will test passing the Auth header to Ozone. Ozone expects an AUTH header and we verify that Ozone responds correctly in case the header is present or returns error if it is missing. export Ozone_VERBOSE if you want to see the XML erorr payload

3) test_bad_bucket_names.sh -> verifies that Ozone returns errors on invalid bucket names

