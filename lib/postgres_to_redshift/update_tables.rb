module PostgresToRedshift
  class UpdateTables
    def initialize(bucket:, source_uri:, target_uri:, schema:)
      @bucket = bucket
      @source_uri = source_uri
      @target_uri = target_uri
      @schema = schema
    end

    def incremental
      start_time_of_previous_job = Time.parse(File.read(PostgresToRedshift::TIMESTAMP_FILE_NAME)).utc
      with_tracking do |incremental_to|
        with_retry do
          in_transaction do
            tables.each do |table|
              incremental_from = table.dirty? ? CopyImport::BEGINNING_OF_TIME : start_time_of_previous_job
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

    def tables
      # do not cache - we want fresh table listing for retries
      Tables.new(source_connection: source_connection, target_connection: target_connection).all
    end

    def disconnect
      target_connection.exec('ROLLBACK;') rescue nil
      %i[@source_connection @target_connection].each do |connection_variable|
        next unless instance_variable_defined?(connection_variable)

        connection = remove_instance_variable(connection_variable)
        connection.finish rescue nil
      end
    end

    def with_retry
      retries_remaining = 2
      begin
        yield
      rescue StandardError => e
        puts "Import failed with #{retries_remaining} retries remaining due to: #{e.message}"
        disconnect
        raise unless retries_remaining.positive?

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
