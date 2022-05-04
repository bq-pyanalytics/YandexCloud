import os, random, telebot


bot = telebot.TeleBot(os.environ.get('BOT_TOKEN'))


dialog = {
    'hello': {
        'in': ['/hello', 'привет', 'hello', 'hi', 'privet', 'hey'],
        'out': ['Приветствую', 'Здравствуйте', 'Привет!']
    },
    'how r u': {
        'in': ['/howru', 'как дела', 'как ты', 'how are you', 'дела', 'how is it going'],
        'out': ['Хорошо', 'Отлично', 'Good. And how are u?']
    },
    'name': {
        'in': ['/name', 'зовут', 'name', 'имя'],
        'out': [
            'Я telegram-template-bot',
            'Я бот шаблон, но ты можешь звать меня в свой проект',
            'Это секрет. Используй команду /help, чтобы узнать'
        ]
    }
}


@bot.message_handler(commands=['report'])
def say_welcome(message):
    requet_dict = {
        'USER1': ['CLIENT1', "CLIENT2", 'CLIENT3'],
        'USER2': ['CLIENT4', "CLIENT5", "CLIENT6"]
    }

    response = ', '.join(requet_dict[message.from_user.username])
    bot.send_message(message.chat.id, f"Hi, {response}", parse_mode='markdown')


@bot.message_handler(commands=['help', 'start', 'logs'])
def say_welcome(message):
    bot.send_message(message.chat.id, f"Hi, {message.from_user.username}", parse_mode='markdown')


@bot.message_handler(func=lambda message: True)
def echo(message):
    for t, resp in dialog.items():
        if sum([e in message.text.lower() for e in resp['in']]):
            bot.send_message(message.chat.id, random.choice(resp['out']))
            return

    bot.send_message(message.chat.id, f"Hi, {message.from_user.username}")


if __name__ == '__main__':
    bot.infinity_polling()
