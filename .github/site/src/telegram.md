# Telegram

<details>
  <summary>Bot creation</summary>

1) Create a bot - see https://core.telegram.org/bots/features#creating-a-new-bot
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain a `chat_id` following the accepted answer https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel
</details>

Example configuration:

```toml
messenger.specific.chat_id = "-100129008371"
messenger.url = "https://api.telegram.org/bot<bot token>/sendMessage"
```

[Rate limit](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)

*Note* for Telegram all info-level messages are sent without notification so the phone doesn't vibrate or make any sound.