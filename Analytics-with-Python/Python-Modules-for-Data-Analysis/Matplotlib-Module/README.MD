# Matplotlib Library

## Table Of Contents
- [Types of Charts using Matplotlib]() 
  1. [3D Scatter Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/3D-Scatter-Chart-with-Matplotlib.ipynb)
  2. [Area Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Area-Charts-with-Matplotlib.ipynb) 
  3. [Bar Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Bar-Chart-with-Matplotlib.ipynb) 
  4. [Boxplot Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Boxplot-Chart-with-Matplotlib.ipynb) 
  5. [Heatmap Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Heatmap-with-Matplotlib.ipynb) 
  6. [Histogram Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Histogram-Chart-with-Matplotlib.ipynb) 
  7. [Line Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Line-Chart-with-Matplotlib.ipynb) 
  8. [Pie Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Pie-Chart-with-Matplotlib.ipynb) 
  9. [Table Charts](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/Table-with-Matplotlib.ipynb) 
  10. [World Map](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Matplotlib-Module/World-Map-Chart-with-Matplotlib.ipynb)
  
- [Further Reading]()
  - [Dev Community - Subplotting with matplotlib and seaborn](https://dev.to/thalesbruno/subplotting-with-matplotlib-and-seaborn-5ei8)
  - [Towards Data Science - Make Your Charts Look Glorious](https://towardsdatascience.com/make-your-charts-look-glorious-9ce3fa310b70)

# Data Description
1. [Grouped Country DataFrame](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Analytics-with-Python/Exploratory-Data-Analysis-with-Python/Exploratory-Data-Analysis-for-Online-Retail-Store/grouped_country_data.csv)
2. [Grouped Daily Country DataFrame](https://github.com/nyangweso-rodgers/Data_Analytics/blob/main/Analytics-with-Python/Exploratory-Data-Analysis-with-Python/Exploratory-Data-Analysis-for-Online-Retail-Store/grouped_daily_country_df.csv)
3. [Grouped Daily Customers Data](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Analytics-with-Python/Exploratory-Data-Analysis-with-Python/Exploratory-Data-Analysis-for-Online-Retail-Store/grouped_daily_customer_data.csv)

# Elemets of Matplotlib
1. __Figure__: This of this as a canvas that the chart(s) is / are placed onto.
2. __Figure Title__: The title of the figure. This can be different to the title given to a chart (or axes). This is not shown on the above example.
3. __Axes__: An axes is the container for a chart (also called a plot). An axes sits on top of the figure and there can be more than one axes on a figure.
4. Axes Title: This is the title for the axes.
5. Y-Axis Label: The label that describes what the y-axis represents.
6. X-Axis Label: Does the same as the y-axis label, only it's for the x-axis.
7. Tick: What the data represents from the data source (for example, what currency does the bar represent).
8. Legend: A list of what each data point on the plot/chart is.

# Histogram
* A __histogram__ is an accurate representation of the numerical date. It is an _estimation of the probability distribution of a continuous variable_.
* To construct a histogram, follow the following steps:
  1. Bin, the range of values.
  2. Divide the entire range of values into a series of intervals.
  3. Count how many values fall into each interval.

# Parameters for Histogram
* In __Matplotlib__, we use the __hist()__ function to create histograms. "The __hist()__ function will use an array of numbers to create a histogram, the array is sent into the function as an argument.