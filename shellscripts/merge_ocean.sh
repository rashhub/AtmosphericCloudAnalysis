cd /home/rashnil/bigdata/cdiac.ornl.gov/ftp/ndp026b/unpacked_ocean_data

for file in $(ls *)
do

#file_name=$(echo "$file" | cut -f1-2 -d '.')

#if [ ${file_seq:0:1} -lt 3 ]; then

cat $file >> merged_ocean_data.DAT

#echo "Processed : "$file

#fi

done

