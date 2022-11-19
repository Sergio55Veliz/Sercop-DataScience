import time
import sys
from colorama import Fore as color

def progress_bar(actual_value,
                 max_value,
                 size_bar=50,
                 character='■',
                 initial_message="Progress:",
                 end_message='',
                 error_indicator=False):

    percent = actual_value/max_value
    message = '\r' + initial_message + ' [{0:'+str(size_bar)+'s}] {1:6d}/{2:1d} ' + end_message
    if percent<1:
        if error_indicator:
            sys.stdout.write(color.LIGHTRED_EX + message.format(character * int(size_bar * percent), actual_value, max_value))
        else:
            sys.stdout.write(color.LIGHTYELLOW_EX + message.format(character * int(size_bar * percent), actual_value, max_value))
        sys.stdout.flush()
    else:
        sys.stdout.write(color.BLUE + message.format(character * int(size_bar * percent), actual_value, max_value))
        sys.stdout.flush()

    if percent == 1: print(color.RESET)
    # Formato:
    '''
    pre_message
    initial_message [■■■■■■■■                                          ]    230/151100 end_message
    post_message
    '''

def get_str_time_running(t_begin):
    t_running = time.time() - t_begin
    hours = int(t_running / (60 * 60))
    minutes = int(t_running / 60 - 60 * int(t_running / (60 * 60)))
    seconds = int(t_running - 60 * int(t_running / 60))
    return '0'*(2-len(str(hours)))+str(hours)+'h ' + '0'*(2-len(str(minutes)))+str(minutes)+'min ' + '0'*(2-len(str(seconds)))+str(seconds) + 's'