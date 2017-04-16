cd /home/rashnil/bigdata/cdiac.ornl.gov/ftp/ndp026b/land_data

for file in $(ls *)
do

file_name=$(echo "$file" | cut -f1-2 -d '.')

#if [ ${file_seq:0:1} -lt 3 ]; then

zcat $file > ../unpacked_land_data/$file_name

#echo "Processed : "$file

#fi

done

