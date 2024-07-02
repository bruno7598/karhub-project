from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Processamento de Dados") \
        .getOrCreate()

    input_path = "./resources/csvs/gdvDespesasExcel.csv"
    
    output_path = "./resources/parquet/gdvDespesasExcel.parquet"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.printSchema()

    row_count = df.count()
    print(f"Total de linhas no DataFrame: {row_count}")

    df.write.parquet(output_path, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()