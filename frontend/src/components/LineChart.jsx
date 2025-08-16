import { useTheme } from "@mui/material";
import { ResponsiveLine } from "@nivo/line";
import { tokens } from "../theme";
import { useState, useEffect } from "react";
import { Box, CircularProgress, Typography } from "@mui/material";

// Transform API response data to match Nivo's expected format for ResponsiveLine component
const transformDataForNivo = (apiData, chartType) => {
  console.log("Raw API data:", apiData);
  console.log("Chart type:", chartType);

  if (!Array.isArray(apiData) || apiData.length === 0) {
    console.log("Data is not an array or is empty");
    return [];
  }

  // For transactions-by-hour data format: [{hour: 0, count: 235608}, ...]
  if (chartType === "transactions-by-hour") {
    const transformed = [
      {
        id: "Transactions",
        data: apiData.map(item => ({
          x: item.hour.toString(), // Convert to string for point scale
          y: item.count
        }))
      }
    ];
    console.log("Transformed data:", transformed);
    return transformed;
  }

  // For age-distribution-line data format: [{age: 18, count: 61, cluster: null}, ...]
  if (chartType === "age-distribution-line" || chartType === "age-distribution") {
    // Helper function to determine age bin
    const getAgeBin = (age) => {
      if (age >= 18 && age <= 25) return "18-25";
      if (age >= 26 && age <= 35) return "26-35";
      if (age >= 36 && age <= 45) return "36-45";
      if (age >= 46 && age <= 55) return "46-55";
      if (age >= 56 && age <= 65) return "56-65";
      if (age >= 66) return "66+";
      return "Unknown";
    };

    // Group data by cluster and age bins
    const groupedByCluster = {};

    apiData.forEach(item => {
      const clusterKey = item.cluster || "No Cluster";
      const ageBin = getAgeBin(item.age);

      if (!groupedByCluster[clusterKey]) {
        groupedByCluster[clusterKey] = {};
      }

      // Sum counts for the same age bin and cluster
      if (groupedByCluster[clusterKey][ageBin]) {
        groupedByCluster[clusterKey][ageBin] += item.count;
      } else {
        groupedByCluster[clusterKey][ageBin] = item.count;
      }
    });

    // Define age bin order for consistent display
    const ageBinOrder = ["18-25", "26-35", "36-45", "46-55", "56-65", "66+"];

    // Transform to Nivo format - separate line for each cluster
    const transformed = Object.keys(groupedByCluster).map(cluster => ({
      id: cluster,
      data: ageBinOrder
        .filter(ageBin => groupedByCluster[cluster][ageBin]) // Only include bins with data
        .map(ageBin => ({
          x: ageBin,
          y: groupedByCluster[cluster][ageBin]
        }))
    }));

    console.log("Transformed age distribution data with bins:", transformed);
    return transformed;
  }

  // For other chart types, you can add similar transformations
  // This is a fallback for other formats
  const fallback = [
    {
      id: "Data",
      data: apiData.map((item, index) => ({
        x: item.x || item.hour || index,
        y: item.y || item.count || item.value
      }))
    }
  ];
  console.log("Fallback transformed data:", fallback);
  return fallback;
};

const LineChart = ({
  isDashboard = false,
  isCustomLineColors = false,
  cluster = null,
  enableFilter = false,
  chartType = "transactions-by-hour", // transactions-by-hour, age-distribution-line, revenue-trends
  title = null
}) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  const [data, setData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);

        // Build the API URL based on chart type
        let url = `http://localhost:8000/api/charts/${chartType}`;
        const params = new URLSearchParams();

        if (cluster) params.append("cluster", cluster);
        if (enableFilter) params.append("enable_filter", "true");

        if (params.toString()) {
          url += `?${params.toString()}`;
        }

        const response = await fetch(url);

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const jsonData = await response.json();

        // Transform data for Nivo ResponsiveLine format
        const transformedData = transformDataForNivo(jsonData, chartType);
        setData(transformedData);
      } catch (err) {
        setError(err.message || "Failed to load chart data");
        console.error("Error fetching line chart data:", err);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [cluster, enableFilter, chartType]); // Re-fetch when these props change

  if (isLoading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  return (
    <ResponsiveLine
      data={data}
      theme={{
        axis: {
          domain: {
            line: {
              stroke: colors.gray[100],
            },
          },
          legend: {
            text: {
              fill: colors.gray[100],
            },
          },
          ticks: {
            line: {
              stroke: colors.gray[100],
              strokeWidth: 1,
            },
            text: {
              fill: colors.gray[100],
            },
          },
        },
        legends: {
          text: {
            fill: colors.gray[100],
          },
        },
        tooltip: {
          container: {
            color: colors.primary[500],
          },
        },
      }}
      colors={{ scheme: "category10" }}
      margin={{ top: 50, right: 110, bottom: 50, left: 60 }}
      xScale={{ type: "point" }}
      yScale={{
        type: "linear",
        min: "auto",
        max: "auto",
        stacked: false,
        reverse: false,
      }}
      yFormat=" >-.2f"
      curve="catmullRom"
      axisTop={null}
      axisRight={null}
      axisBottom={{
        orient: "bottom",
        tickSize: 0,
        tickPadding: 5,
        tickRotation: 0,
        legend: isDashboard ? undefined : getXAxisLabel(chartType),
        legendOffset: 36,
        legendPosition: "middle",
      }}
      axisLeft={{
        orient: "left",
        tickValues: 5,
        tickSize: 3,
        tickPadding: 5,
        tickRotation: 0,
        legend: isDashboard ? undefined : getYAxisLabel(chartType),
        legendOffset: -40,
        legendPosition: "middle",
      }}
      enableGridX={true}
      enableGridY={true}
      pointSize={8}
      pointColor={{ theme: "background" }}
      pointBorderWidth={3}
      pointBorderColor={{ from: "serieColor" }}
      pointLabelYOffset={-12}
      enableArea={false}
      enableSlices="x"
      lineWidth={3}
      useMesh={true}
      legends={[
        {
          anchor: "bottom-right",
          direction: "column",
          justify: false,
          translateX: 100,
          translateY: 0,
          itemsSpacing: 0,
          itemDirection: "left-to-right",
          itemWidth: 80,
          itemHeight: 20,
          itemOpacity: 0.75,
          symbolSize: 12,
          symbolShape: "circle",
          symbolBorderColor: "rgba(0, 0, 0, .5)",
          effects: [
            {
              on: "hover",
              style: {
                itemBackground: "rgba(0, 0, 0, .03)",
                itemOpacity: 1,
              },
            },
          ],
        },
      ]}
    />
  );
};

// Helper functions for axis labels
const getXAxisLabel = (chartType) => {
  switch (chartType) {
    case "transactions-by-hour":
      return "Hour of Day";
    case "age-distribution-line":
    case "age-distribution":
      return "Age";
    case "revenue-trends":
      return "Quarter";
    default:
      return "Time";
  }
};

const getYAxisLabel = (chartType) => {
  switch (chartType) {
    case "transactions-by-hour":
      return "Transaction Count";
    case "age-distribution-line":
    case "age-distribution":
      return "User Count";
    case "revenue-trends":
      return "Revenue ($)";
    default:
      return "Value";
  }
};

export default LineChart;
