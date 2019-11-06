import scimple as scm
import matplotlib.pyplot as plt
import pygal
import scimple.pygal_utils as pyu

s = """
/**
   * N_ITER=1
   * Elapsed time: 18456.667003ms
   * (WAS GraphX,48)
   * Elapsed time: 19235.134488ms
   * (WAS Custom,48)
   * Elapsed time: 21362.89262ms
   * (WAS AggMess,48)
   * Elapsed time: 35702.836235ms
   * (WAS Soundcloud,48)
   * Elapsed time: 85683.263557ms
   * (WAS GraphFrames,48,48)
   *
   * N_ITER=5
   * Elapsed time: 30911.907088ms
   * (WAS GraphX,48)
   * Elapsed time: 42702.515075ms
   * (WAS Custom,48)
   * Elapsed time: 66015.725386ms
   * (WAS AggMess,48)
   * Elapsed time: 97075.823488ms
   * (WAS Soundcloud,48)
   * Elapsed time: 311163.458741ms
   * (WAS GraphFrames,48,48)
   *
   * N_ITER=10
   * Elapsed time: 46336.912898ms
   * (WAS GraphX,48)
   * Elapsed time: 64395.448749ms
   * (WAS Custom,48)
   * Elapsed time: 116064.064586ms
   * (WAS AggMess,48)
   * Elapsed time: 166004.745441ms
   * (WAS Soundcloud,48)
   * Elapsed time: 1400000.0ms
   * (WAS GraphFrames,48,48)
   */
"""
data = {
    "GraphFrames built-in": ([1, 5, 10], [85683, 311163, 1400000]),
    "RDD-based implem from Soundcloud": ([1, 5, 10], [35702, 97075, 166004]),
    "Custom: GraphFrames' AggregateMessages": ([1, 5, 10], [21362, 66015, 116064]),
    "Custom: pure Spark SQL": ([1, 5, 10], [19235, 42702, 64395]),
    "GraphX built-in": ([1, 5, 10], [18456, 30911, 46336]),
}


def plotLine():
    p = scm.Plot(ylabel="running time (ms)",
                 xlabel="n°Iter",
                 title="6M7 nodes, 19M3 edges, 600Mo, on 12 threads CPU, 5go RAM, parallelism/n°shuffle partitions=48",
                 borders=[0, 11, 0, 200000])
    for key, (x, y) in data.items():
        p.add(x=x,
              y=y,
              marker="-",
              label=key,
              colored_area=0.1)

    scm.show(True)


def plotBar(nIter=10):
    p = scm.Plot(ylabel="avg running time (ms) per iteration",
                 xlabel="n°Iter",
                 title="6M7 nodes, 19M3 edges, 600Mo, on 12 threads CPU, 5go RAM, parallelism/n°shuffle partitions=48",
                 )
    p.add(x=list(data.keys()),
          y=[min(e[1][e[0].index(nIter)] / nIter, 200000) for e in data.values()],
          marker="bar",
          # label=key,
          # colored_area=0.1
          )

    scm.show(True)


def plotBarPyg():
    line_chart = pygal.Bar()
    line_chart.truncate_label = len("Custom: GraphFrames' AggregateMessages")
    line_chart.show_legend = False
    line_chart.x_label_rotation = 7
    line_chart.title = "6M7 nodes, 19M3 edges, 600MB, on 12 threads CPU, 5go RAM, parallelism/n°shuffle partitions=48"
    line_chart.x_title = "PageRank implementation"
    line_chart.y_title = "avg running time (ms) per iteration"
    x_labels = list(data.keys())
    x_labels.remove("GraphFrames built-in")
    line_chart.x_labels = x_labels
    for nIter in [1, 5, 10]:
        line_chart.add(f"run of {nIter} iters",
                       [data[algo][1][data[algo][0].index(nIter)] / nIter for algo in data.keys() if
                        algo != "GraphFrames built-in"])
    print(pyu.to_html(line_chart))


def plotShuffleBenchGraphXPartitionings():
    """
    PRGraphXConv(spark, 0.000001) (RESULTAT DETERMINISTES!)
    """

    line_chart = pygal.Bar()
    line_chart.truncate_label = len("CanonicalRandomVertexCut")
    line_chart.truncate_legend = len("Partitioning Shuffle Read/Write")
    # line_chart.show_legend = False
    line_chart.title = "96,7 MB (textFile input=56.4 MB), 4397207 edges, 32276 vertices, on 12 threads CPU, 5go RAM, parallelism=48, PRGraphXConv(tol=0.000001)"
    line_chart.x_title = "PartitioningStrategy"
    line_chart.y_title = "Shuffled data amount MB"
    line_chart.x_labels = ["EdgePartition2D", "EdgePartition1D", "RandomVC", "CanonicalRandomVC", "None"]
    partitioning_shuffle = [41.2, 42.1, 44.6, 44.6, 0]
    processing_shuffle_read = [116.3, 223.1, 260.5, 260.5, 213.1]
    processing_shuffle_write = [114.9, 223.6, 261.1, 261.1, 213.7]
    line_chart.add("Read Processing",
                   [p - ip for p, ip in zip(processing_shuffle_read, partitioning_shuffle)])
    line_chart.add("Write Processing",
                   [p - ip for p, ip in zip(processing_shuffle_write, partitioning_shuffle)])
    line_chart.add("Partitioning",
                   partitioning_shuffle)
    print(pyu.to_html(line_chart))


def plotShuffleBenchGraphXPartitionings():
    """
    PRGraphXConv(spark, 0.000001) (RESULTAT DETERMINISTES!)
    """

    line_chart = pygal.Line(range=(0, 27544))
    line_chart.truncate_label = len("CanonicalRandomVertexCut")
    line_chart.truncate_legend = len("Partitioning Shuffle Read/Write")
    # line_chart.show_legend = False
    line_chart.title = "96,7 MB (textFile input=56.4 MB), 4397207 edges, 32276 vertices, real_max_depth=9, on 12 threads CPU, 5go RAM, parallelism=48, PRGraphXConv(tol=0.000001)"
    line_chart.x_title = "PageRank convergence tolerance"
    line_chart.y_title = "avg running time (ms)"
    pr = {0.000000001: 27544,
          0.00000001: 25984,
          0.0000001: 22608,
          0.000001: sum([16114.451741, 19323.187472, 17090.451325]) / 3,
          0.00001: sum([18225, 11451, 13064]) / 3,
          0.0001: sum([9130, 8668]) / 2,
          0.001: sum([6081, 6615, 5992]) / 3,
          0.01: 3852}
    line_chart.x_labels = list(pr.keys())
    opic_runs = [5734, 3663, 3619, 3535, 3271, 3644]
    opic = [sum(opic_runs) / len(opic_runs) for _ in range(len(pr))]

    line_chart.add("PR", list(pr.values()))
    line_chart.add("full OPIC", opic)
    line_chart.render_in_browser()

    print(pyu.to_html(line_chart))


import html2text
proc = html2text.HTML2Text()
proc.ignore_links = True
proc.ignore_images = True
proc.emphasis_mark = ""
proc.ul_item_mark = ""
proc.strong_mark = ""
docs = {}
import requests
docs["https://www.oncrawl.com/seo-crawler/"] = proc.handle(requests.get("https://www.oncrawl.com/seo-crawler/").text)
print(docs["https://www.oncrawl.com/seo-crawler/"])

