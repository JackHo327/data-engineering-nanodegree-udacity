# please configure the aws credentials before run this script

aws s3 cp /home/workspace/airport-codes_csv.csv s3://denanodegree-s3-bucket/capstone-i94-immigration/
aws s3 cp /home/workspace/us-cities-demographics.csv s3://denanodegree-s3-bucket/capstone-i94-immigration/
aws s3 cp /home/workspace/us-cities-demographics.csv s3://denanodegree-s3-bucket/capstone-i94-immigration/
aws s3 cp --recursive /data/18-83510-I94-Data-2016 s3://denanodegree-s3-bucket/capstone-i94-immigration/i94_immigration_2016_raw/
aws s3 cp /data2/GlobalLandTemperaturesByCity.csv s3://denanodegree-s3-bucket/capstone-i94-immigration/
aws s3 cp --recursive /home/workspace/dictionary_data s3://denanodegree-s3-bucket/capstone-i94-immigration/dictionary_data
