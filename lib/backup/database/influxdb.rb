# encoding: utf-8

module Backup
  module Database
    class Influxdb < Base
      class Error < Backup::Error; end
      
      ##
      # Name of the database that needs to get dumped
      # Leave blank if you want backup only Metastore
      attr_accessor :name

      ##
      # This flag can be used to backup a specific retention policy.
      # If not specified, all retention policies will be backed up.
      attr_accessor :retention

      ##
      # Connectivity options
      attr_accessor :host

      ##
      # This flag can be used to backup a specific shard ID
      # If not specified, all shards will be backed up.
      attr_accessor :shard

      ##
      # This flag can be used to create a backup since a specific date,
      # where the date must be in RFC3339 (e.g 2015-12-24T08:12:23Z)
      attr_accessor :since

      def initialize(model, database_id = nil, &block)
        super
        instance_eval(&block) if block_given?
      end

      def perform!
        super

        pipeline = Pipeline.new
        pipeline << influxdb
        p "Dump path: #{dump_path}"
        p "Dump file name: #{dump_filename}"
        pipeline.run
        if pipeline.success?
          log!(:finished)
        else
          raise Error, "Dump Failed!\n" + pipeline.error_messages
        end
      end

      private

      ##
      # Creates a tar archive of the +dump_packaging_path+ directory
      # and stores it in the +dump_path+ using +dump_filename+.
      #
      #   <trigger>/databases/MongoDB[-<database_id>].tar[.gz]
      #
      # If successful, +dump_packaging_path+ is removed.
      def package!
        pipeline = Pipeline.new
        dump_ext = 'tar'

        pipeline << "#{ utility(:tar) } -cf '#{ dump_filename }' " +
            "-C '#{ dump_path }' ./"

        model.compressor.compress_with do |command, ext|
          pipeline << command
          dump_ext << ext
        end if model.compressor

        pipeline << "#{ utility(:cat) } > " +
            "'#{ File.join(dump_path, dump_filename) }.#{ dump_ext }'"

        pipeline.run
        if pipeline.success?
          FileUtils.rm_rf dump_packaging_path
          log!(:finished)
        else
          raise Error, "Dump Failed!\n" + pipeline.error_messages
        end
      end

      def influxdb
        "#{utility(:influxd)} backup #{database_option} #{connectivity_options} #{dump_path}"
      end

      def connectivity_options
        opts = []
        opts = "-host #{host}" if host
        opts.join(' ')
      end

      def database_option
        opts = []
        opts << "-database #{name}" if name
        opts << "-shard #{shard}" if shard
        opts << "-retention #{retention}" if retention
        opts << "-since #{since}" if since
        opts.join(' ')
      end

      def dump_packaging_path
        File.join(dump_path, dump_filename)
      end
    end
  end
end
