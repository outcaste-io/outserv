rm $2.json.gz
for i in `seq $1 $2`; do
  echo $i
  cat query.txt |\
  sed "s/BNUM/$i/" |\
  curl -s $URL -XPOST -H "Content-Type: application/json" -d @- |\
  jq -c .data.block | pigz >> $2.json.gz
done
