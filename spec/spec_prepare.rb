module PostgresToRedshift::Test
  def self.source_uri
    PostgresToRedshift.source_uri
  end

  def self.target_uri
    PostgresToRedshift.target_uri
  end

  def self.test_connection
    @test_connection ||= PG::Connection.new(
      host: source_uri.host,
      port: source_uri.port,
      user: source_uri.user || ENV['USER'],
      password: source_uri.password,
      dbname: source_uri.path[1..-1]
    )
  end

  def self.test_target_connection
    @test_target_connection ||= PG::Connection.new(
      host: target_uri.host,
      port: target_uri.port,
      user: target_uri.user || ENV['USER'],
      password: target_uri.password,
      dbname: target_uri.path[1..-1]
    )
  end
end

RSpec.configure do |config|
  config.before :suite do
    PostgresToRedshift::Test.test_connection
  end
end
