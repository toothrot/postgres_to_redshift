module PostgresToRedshift
  class FullImport
    def initialize(table:, target_connection:, schema:)
      @table = table
      @target_connection = target_connection
      @schema = schema
    end

    def run
      puts "Importing #{table.target_table_name}"

      target_connection.exec('BEGIN;')

      target_connection.exec("CREATE TABLE IF NOT EXISTS #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")

      target_connection.exec("ALTER TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

      target_connection.exec("CREATE TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      target_connection.exec("COPY #{schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")

      if PostgresToRedshift.dry_run?
        target_connection.exec('ROLLBACK;')
      else
        target_connection.exec('COMMIT;')
      end
    end

    private

    attr_reader :table, :target_connection, :schema
  end
end
