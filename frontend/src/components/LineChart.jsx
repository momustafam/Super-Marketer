import { useTheme } from "@mui/material";
import { ResponsiveLine } from "@nivo/line";
import { tokens } from "../theme";
import { useState, useEffect } from "react";
import { Box, CircularProgress, Typography } from "@mui/material";

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
        let url = `http://localhost:8000/api/charts/${chartType}/test`;
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
        setData(jsonData);
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
      colors={isDashboard ? { datum: "color" } : { scheme: "nivo" }}
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
      enableGridX={false}
      enableGridY={false}
      pointSize={8}
      pointColor={{ theme: "background" }}
      pointBorderWidth={2}
      pointBorderColor={{ from: "serieColor" }}
      pointLabelYOffset={-12}
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
      return "Month";
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
      return "User Count";
    case "revenue-trends":
      return "Revenue ($)";
    default:
      return "Value";
  }
};

export default LineChart;