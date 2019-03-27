require 'bundler/gem_tasks'

begin
  require 'rspec/core/rake_task'
  namespace :spec do
    RSpec::Core::RakeTask.new(:units) do |t|
      t.rspec_opts = '--tag ~type:feature'
    end
    RSpec::Core::RakeTask.new(:features) do |t|
      t.rspec_opts = '--tag type:feature'
    end
  end
end

task(:default).clear
task default: 'spec:units'
