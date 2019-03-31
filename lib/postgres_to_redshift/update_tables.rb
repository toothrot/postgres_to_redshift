module PostgresToRedshift
  class UpdateTables
    def initialize(bucket:, source_uri:, target_uri:, schema:)
      @bucket = bucket
      @source_uri = source_uri
      @target_uri = target_uri
      @schema = schema
    end

    def incremental
      incremental_from = Time.parse(File.read(PostgresToRedshift::TIMESTAMP_FILE_NAME)).utc
      with_tracking do |incremental_to|
        with_retry do
          in_transaction do
            tables.each do |table|
              CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema, incremental_from: incremental_from, incremental_to: incremental_to).run
            end
          end
        end
      end
    end

    def full
      with_tracking do |incremental_to|
        tables.each do |table|
          with_retry do
            in_transaction do
              CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema, incremental_to: incremental_to).run
            end
          end
        end
      end
    end

    private

    def source_connection
      unless instance_variable_defined?(:"@source_connection")
        @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
        @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
      end

      @source_connection
    end

    def target_connection
      @target_connection ||= PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    def column_definitions(table)
      source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
    end

    def tables
      @tables ||= source_connection.exec(tables_sql).map do |table_attributes|
        table = Table.new(attributes: table_attributes)
        table.columns = column_definitions(table)
        table
      end
    end

    def redshift_include_tables
      @redshift_include_tables ||= ENV['REDSHIFT_INCLUDE_TABLES'].split(',')
    end

    def tables_sql
      sql = "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW') AND table_name !~* '^pg_.*'"
      if ENV['REDSHIFT_INCLUDE_TABLES']
        table_names = "'" + redshift_include_tables.join("', '") + "'"
        sql += " AND table_name IN (#{table_names})"
      end
      sql += ' ORDER BY table_name'
      sql
    end

    def with_retry
      retries_remaining = 2
      begin
        yield
      rescue StandardError => e
        puts "Import failed with #{retries_remaining} retries remaining due to: #{e.message}"
        target_connection.exec('ROLLBACK;') rescue nil
        raise unless retries_remaining.positive?

        remove_instance_variable(:"@source_connection") if instance_variable_defined?(:"@source_connection")
        remove_instance_variable(:"@target_connection") if instance_variable_defined?(:"@target_connection")
        sleep 30
        retries_remaining -= 1
        retry
      end
    end

    def with_tracking
      start_time = Time.now.utc
      puts "Import started at #{start_time}"
      yield start_time
      File.write(PostgresToRedshift::TIMESTAMP_FILE_NAME, start_time.iso8601)
    end

    def in_transaction
      target_connection.exec('BEGIN;')
      yield
      if PostgresToRedshift.dry_run?
        target_connection.exec('ROLLBACK;')
        puts 'Rolled back'
      else
        target_connection.exec('COMMIT;')
        puts 'Committed'
      end
    end

    attr_reader :bucket, :source_uri, :target_uri, :schema
  end
end
