mvn clean deploy
if [ $? -ne 0 ]; then
  echo "Maven deploy failed"
fi

curl -X POST \
  'https://ossrh-staging-api.central.sonatype.com/manual/upload/defaultRepository/com.newrelic.labs?publishing_type=user_managed' \
  -H "Authorization: Bearer $OSSRH_LABS_TOKEN" \
  -d ''
if [ $? -ne 0 ]; then
  echo "Curl upload failed"
fi
