from simulator.Configuration import Configuration


class AliCloud(object):
    """
    DataCenter: China North 1
    """
    # 8 vCPU, normal VM, $/month
    computing = 128.8
    # normal HDD, $/(TiB*month)
    storage = 43.008
    # $/TiB
    bandwidth = 112.64


class Amazon(object):
    """
    DataCenter: US EAST, Ohio
    """
    # 8 vCPU, normal VM, $/month
    computing = 247.68
    # cold HDD, $/(TiB*month)
    storage = 25.6
    # $/TiB
    bandwidth = 10.24


class Azure(object):
    """
    DataCenter: US EAST 2
    """
    # 8 vCPU, normal VM, $/month
    computing = 192.69
    # normal HDD, $/(TiB*month)
    storage = 40.96
    # $/TiB
    bandwidth = 10.24


class TC(object):
    """
    Total cost for storage, bandwidth and computing, in dollars($).
    """

    # storage: in TiB*month, bandwidth in TiBs, computing in number of nodes
    def cost(self, storage, bandwidth, computing=0):
        cost = {}
        cost["Ali"] = (storage*AliCloud.storage, bandwidth*AliCloud.bandwidth, computing*AliCloud.computing)
        cost["Amazon"] = (storage*Amazon.storage, bandwidth*Amazon.bandwidth, computing*Amazon.computing)
        cost["Azure"] = (storage*Azure.storage, bandwidth*Azure.bandwidth, computing*Azure.computing)

        for csp in cost.keys():
            for item in cost[csp]:
                print format(item, ".4e")
            print "sum:", sum(cost[csp])
        return cost

    def SLA(self, reliability):
        pass


if __name__ == "__main__":
    tc = TC()
    tc.cost(15*12*pow(2,10), 8e4, 1440)
