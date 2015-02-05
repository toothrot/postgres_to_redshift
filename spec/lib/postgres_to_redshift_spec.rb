require 'spec_helper'

RSpec.describe PostgresToRedshift do
  it 'opens a read only connection to source database' do
    read_only_state = PostgresToRedshift.source_connection.exec("SHOW transaction_read_only").first["transaction_read_only"]

    expect(read_only_state).to eq("on")
  end

  context 'with a simple table' do
    before do
      PostgresToRedshift::Test.test_connection.exec(%Q[DROP TABLE IF EXISTS "films"; CREATE TABLE IF NOT EXISTS "films" ("id" SERIAL PRIMARY KEY, "title" text);])
    end

    it 'lists available tables' do
      expect(PostgresToRedshift.new.tables.size).to eq(1)
      expect(PostgresToRedshift.new.tables.first.name).to eq("films")
    end
  end
end
