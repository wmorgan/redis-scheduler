require 'rubygems'
require 'rubygems/package_task'

spec = Gem::Specification.new do |s|
 s.name = "redis-scheduler"
 s.version = "0.1"
 s.date = Time.now
 s.email = "wmorgan-redis-scheduler@masanjin.net"
 s.authors = ["William Morgan"]
 s.summary  = "A basic chronological scheduler for Redis."
 s.description = "A basic chronological scheduler for Redis. Add work items to be processed at specific times in the future, and easily retrieve all items that are ready for processing."
 s.homepage = "http://gitub.com/wmorgan/redis-scheduler"
 s.files = %w(README COPYING lib/redis-scheduler.rb)
 s.executables = []
 #s.extra_rdoc_files = %w(README)
 #s.rdoc_options = %w(-c utf8 --main README --title Redis)
end

task :rdoc do |t|
  sh "rm -rf doc; rdoc #{spec.rdoc_options.join(' ')} #{spec.extra_rdoc_files.join(' ')} lib/whistlepig.rb"
end

Gem::PackageTask.new spec do
end
