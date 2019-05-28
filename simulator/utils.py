

def splitMethod(string, split_with=','):
    s = string.strip()
    return s.split(split_with)

def splitIntMethod(string, split_with=','):
    res = []
    s = string.strip()
    tmp = s.split(split_with)
    for item in tmp:
        res.append(int(item))

    return res

def extractDRS(string):
    drs = splitMethod(string, '_')
    return [drs[0]] + [int(item) for item in drs[1:]]

def extractHeteDRSFromDiffMedium(string):
    drs = splitMethod(string, ':')
    res = []
    for item in drs:
        medium_drs = []
        item1 = item.replace('(', ' ')
        item2 = item1.replace(')', ' ')
        for s_item in splitMethod(item2):
            medium_drs.append(extractDRS(s_item))
        res.append(medium_drs)

    return res


if __name__ == "__main__":
    s = " (RS_3_1, RS_14_10) : (RS_14_10, LRC_16_10_2)"
    r = extractHeteDRSFromDiffMedium(s)
    print r
