module PostgresToRedshift
  class IncrementalImport
    def initialize(table:, target_connection:, schema:)
      @table = table
      @target_connection = target_connection
      @schema = schema
    end

    def run
      puts "Importing #{table.target_table_name}"

      target_connection.exec("CREATE TABLE IF NOT EXISTS #{table_name} (#{table.columns_for_create});")

      target_connection.exec("CREATE TEMPORARY TABLE #{temp_table_name} (#{table.columns_for_create});")

      target_connection.exec("COPY #{temp_table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec("DELETE FROM #{table_name} USING #{temp_table_name} source WHERE #{table_name}.id = source.id;")

      target_connection.exec("INSERT INTO #{table_name} SELECT * FROM #{temp_table_name};")
    end

    private

    def table_name
      "#{schema}.#{target_connection.quote_ident(table.target_table_name)}"
    end

    def temp_table_name
      target_connection.quote_ident(table.target_table_name)
    end

    attr_reader :table, :target_connection, :schema
  end
end
