require 'spec_helper'

RSpec.describe 'a small source database', type: :feature do
  context 'with a simple column' do
    before(:all) do
      PostgresToRedshift::Test.test_connection.exec(<<-EOS
        DROP TABLE IF EXISTS "p2r_integration";
        CREATE TABLE IF NOT EXISTS "p2r_integration" ("id" SERIAL PRIMARY KEY, "title" text);
        INSERT INTO "p2r_integration" ("title") VALUES ('Casablanca');
      EOS
                                                   )
    end
    after(:all) do
      PostgresToRedshift::Test.test_connection.exec('DROP TABLE IF EXISTS "p2r_integration";')
      PostgresToRedshift::Test.test_target_connection.exec('DROP TABLE IF EXISTS "p2r_integration";')
    end

    it 'Copies all rows to target table' do
      PostgresToRedshift.update_tables
      result = PostgresToRedshift::Test.test_target_connection.exec(
        'SELECT * FROM "p2r_integration";'
      )
      expect(result.num_tuples).to eq(1)
      expect(result[0]).to eq('title' => 'Casablanca', 'id' => '1')
    end
  end
end
