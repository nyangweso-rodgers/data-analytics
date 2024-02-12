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

1. **Figure**: This of this as a canvas that the chart(s) is / are placed onto.
2. **Figure Title**: The title of the figure. This can be different to the title given to a chart (or axes). This is not shown on the above example.
3. **Axes**: An axes is the container for a chart (also called a plot). An axes sits on top of the figure and there can be more than one axes on a figure.
4. Axes Title: This is the title for the axes.
5. Y-Axis Label: The label that describes what the y-axis represents.
6. X-Axis Label: Does the same as the y-axis label, only it's for the x-axis.
7. Tick: What the data represents from the data source (for example, what currency does the bar represent).
8. Legend: A list of what each data point on the plot/chart is.

# Histogram

- A **histogram** is an accurate representation of the numerical date. It is an _estimation of the probability distribution of a continuous variable_.
- To construct a histogram, follow the following steps:
  1. Bin, the range of values.
  2. Divide the entire range of values into a series of intervals.
  3. Count how many values fall into each interval.

# Parameters for Histogram

- In **Matplotlib**, we use the **hist()** function to create histograms. "The **hist()** function will use an array of numbers to create a histogram, the array is sent into the function as an argument.

## Box Plots

- Used to display the distribution and key characteristics of a dataset.
- They provide a clear and concise summary of:

  1. Central tendency
  2. Spread
  3. Outliers

- **Features** include:
  - The box in a box plot represents the **interquartile range** (IQR), which contains the middle 50% of the data.
  - The line inside the box represents the **median**, dividing the data into two equal halves. It gives us a measure of the dataset's **central tendency**.
  - The **whiskers** in a box plot extend from the box and represent the **variability** of the data. They usually reach the smallest and largest non-outlier observations.
  - **Outliers**, shown as individual points or symbols, lie outside the **whiskers** and highlight extreme values.
  - Box plots can also provide insights into the **symmetry** or **skewness** of the data distribution. If the **median** is not in the center of the box, it suggests a **skewness** towards the longer whisker. This helps us detect departures from a symmetric distribution.
- **Comparability**
  - Another valuable aspect of box plots is their ability to compare distributions.
  - We can create side-by-side box plots to compare multiple datasets, making it easy to identify differences in **medians**, **spreads**, and **outliers**.
