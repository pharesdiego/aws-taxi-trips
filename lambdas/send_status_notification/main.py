import boto3
import json
import random
import os

helpful_and_important_tokens_for_inferring_machine_status_without_looking_at_email_content = [
    '( ͡° ͜ʖ ͡°)',
    '¯_(ツ)_/¯',
    "̿̿ ̿̿ ̿̿ ̿'̿'̵͇̿̿З= ( ▀ ͜͞ʖ▀) =Ε/̵͇̿̿/’̿’̿ ̿ ̿̿ ̿̿ ̿̿",
    '▄︻̷̿┻̿═━一',
    '( ͡°( ͡° ͜ʖ( ͡° ͜ʖ ͡°)ʖ ͡°) ͡°)',
    'ʕ•ᴥ•ʔ',
    '(▀̿Ĺ̯▀̿ ̿)',
    '(ง ͠° ͟ل͜ ͡°)ง',
    '༼ つ ◕_◕ ༽つ',
    'ಠ_ಠ',
    '(づ｡◕‿‿◕｡)づ',
    "̿'̿'̵͇̿̿З=( ͠° ͟ʖ ͡°)=Ε/̵͇̿̿/'̿̿ ̿ ̿ ̿ ̿ ̿",
    '(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧ ✧ﾟ･: *ヽ(◕ヮ◕ヽ)',
    '[̲̅$̲̅(̲̅5̲̅)̲̅$̲̅]',
    '┬┴┬┴┤ ͜ʖ ͡°) ├┬┴┬┴',
    '( ͡°╭͜ʖ╮͡° )',
    '(͡ ͡° ͜ つ ͡͡°)',
    '(• Ε •)',
    "(ง'̀-'́)ง",
    '(ಥ﹏ಥ)',
    "﴾͡๏̯͡๏﴿ O'RLY?",
    '(ノಠ益ಠ)ノ彡┻━┻',
    '[̲̅$̲̅(̲̅ ͡° ͜ʖ ͡°̲̅)̲̅$̲̅]',
    '(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧',
    '(☞ﾟ∀ﾟ)☞',
    '| (• ◡•)| (❍ᴥ❍Ʋ)',
    '(◕‿◕✿)',
    '(ᵔᴥᵔ)',
    '(¬‿¬)',
    '(☞ﾟヮﾟ)☞ ☜(ﾟヮﾟ☜)',
    '(づ￣ ³￣)づ',
    'ლ(ಠ益ಠლ)',
    'ಠ╭╮ಠ',
    "̿ ̿ ̿'̿'̵͇̿̿З=(•_•)=Ε/̵͇̿̿/'̿'̿ ̿"
]


def handler(event, context):
    sns = boto3.client('sns')
    the_chosen_one = random.choice(
        helpful_and_important_tokens_for_inferring_machine_status_without_looking_at_email_content)

    subject = f'Taxi Trips State Machine changed status to {event["detail"]["status"]}={the_chosen_one}'
    message = f"""
    Your details (づ｡◕‿‿◕｡)づ:

    {json.dumps(event,
    indent=2)}
    """

    sns.publish(
        TopicArn=os.environ['TOPIC_ARN'],
        Message=message,
        Subject=subject
    )
