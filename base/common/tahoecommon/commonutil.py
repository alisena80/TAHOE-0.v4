# Common utility functions
import os
import sys
from datetime import date
from datetime import datetime

def getdaytime():
    """
    Get current date time string
    """
    today = date.today()
    now = datetime.now()
    current_time = now.strftime("%H-%M-%S")
    text = today.isoformat() + "-" + current_time
    return text

def getCurrentYear():
    """
    Get year
    """
    today = date.today()
    return today.year