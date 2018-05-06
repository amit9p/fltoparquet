# fltoparquet
fixed length file to csv and parquet converter
This application converts fixed length file to parquet and csv
After importing the project in eclipse add scala nature to project and scala version to 2.11
The application takes 4 parameter :
1.Input fixed length file path (ex. "/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/fixedlength.txt")
2.output csv file path ("/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/csvFile.txt")
3.output parquet file path ("/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/csvFile.parquet")
4.Input Fixed length schema in xlsx ("/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/schema.xlsx")

Command to run the jar:
java -jar flparquetCoverter.jar "/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/fixedlength.txt" "/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/csvFile.txt" "/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/csvFile.parquet" "/home/amitprasad/MyFolder/Tech/fixedlengthToParquet/schema.xlsx"

Sample fixed length file and schema is present in /src/main/resources/
