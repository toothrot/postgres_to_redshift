module PostgresToRedshift
  class FullImport
    def initialize(table:, target_connection:, schema:)
      @table = table
      @target_connection = target_connection
      @schema = schema
    end

    def run
      puts "Importing #{table.target_table_name} at #{Time.now.utc}"

      # TRUNCATE cannot be rolled back
      target_connection.exec("DROP TABLE IF EXISTS #{table_name};")

      target_connection.exec("CREATE TABLE #{table_name} (#{table.columns_for_create});")

      target_connection.exec("COPY #{table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec("ANALYZE #{table_name};")
    end

    private

    def table_name
      "#{schema}.#{target_connection.quote_ident(table.target_table_name)}"
    end

    attr_reader :table, :target_connection, :schema
  end
end
