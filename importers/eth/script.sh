rm $2.json
for i in `seq $1 $2`; do cat query.txt |\
  sed "s/BNUM/$i/" |\
  curl $URL -XPOST -H "Content-Type: application/json" -d @- |\
  jq -c .data.block >> $2.json
done
