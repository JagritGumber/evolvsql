import Config

config :wire, port: 5433

config :logger, :console,
  level: :info,
  format: "$time [$level] $message\n"
