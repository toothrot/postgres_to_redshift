module PostgresToRedshift::Tests
  def self.test_uri
    URI.parse(ENV['POSTGRES_TO_REDSHIFT_TEST_URI'])
  end

  def self.connection
    @connection ||= PG::Connection.new(
      host: test_uri.host,
      port: test_uri.port,
      user: test_uri.user,
      password: test_uri.password,
      dbname: test_uri.path[1..-1])
  end
end

RSpec.configure do |config|
  config.before :suite do
    PostgresToRedshift::Tests.connection
  end
end
