from simulator.drs.RS import RS
from simulator.drs.LRC import LRC
from simulator.drs.XORBAS import XORBAS
from simulator.drs.MSR import MSR
from simulator.drs.MBR import MBR
from simulator.drs.DRC import DRC


def getDRSHandler(redun_name, params):
    if redun_name.upper() == "RS":
        handler = RS(params)
    elif redun_name.upper() == "LRC":
        handler = LRC(params)
    elif redun_name.upper() == "XORBAS":
        handler = XORBAS(params)
    elif redun_name.upper() == "MSR":
        handler = MSR(params)
    elif redun_name.upper() == "MBR":
        handler = MBR(params)
    elif redun_name.upper() == "DRC":
        handler = DRC(params)
    else:
        raise Exception("Incorrect data redundancy name!")
    return handler


if __name__ == "__main__":
    redun_name = "MBR"
    params = [9,6,7]
    handler = getDRSHandler(redun_name, params)
    print handler.SSC
