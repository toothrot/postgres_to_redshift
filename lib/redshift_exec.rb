require "helper/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'slack-notifier'
require 'zlib'
require 'tempfile'
require "helper/slack_notifier"
#require "pry-rails"

class RedshiftExec
  def self.exec_sql
    executable_file = s3.buckets[bucket_name].objects[object_name]
    puts ("Executing Script on #{bucket_name}/#{object_name}: #{executable_file}")
    target_connection.exec(executable_file.read)

    if (RedshiftExec.slack_on_success == 'true')
      message = "[REXE]SUCCESS: Script executed on RedShift | BUCKET: #{RedshiftExec.bucket_name} | SCRIPT: #{RedshiftExec.object_name}"
      SLACK_NOTIFIER.ping message
    end
  rescue => e
    SLACK_NOTIFIER.ping "[REXE]#{e.message.gsub("\r"," ").gsub("\n"," ")} | BUCKET: #{RedshiftExec.bucket_name} | SCRIPT: #{RedshiftExec.object_name}"
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['P2RS_TARGET_URI'])
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end
    @target_connection
  end

  def target_connection
    self.class.target_connection
  end

  def self.s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['P2RS_S3_EXPORT_ID'], secret_access_key: ENV['P2RS_S3_EXPORT_KEY'])
  end

  def self.bucket_name
    @bucket_name ||= ENV['REXE_S3_SCRIPT_BUCKET']
  end

  def self.object_name
    @object_name ||= ENV['REXE_S3_SCRIPT_NAME']
  end

  def self.slack_on_success
    @slack_on_success ||= ENV['SLACK_ON_SUCCESS']
  end
end
