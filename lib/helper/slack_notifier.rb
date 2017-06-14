require "slack-notifier"

::SLACK_NOTIFIER = Slack::Notifier.new(ENV["SLACK_WEBHOOK_URL"],
                                       channel: ENV["SLACK_CHANNEL"],
                                       username: ENV["SLACK_USERNAME"])
