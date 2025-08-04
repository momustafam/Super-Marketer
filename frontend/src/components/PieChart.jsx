import { ResponsivePie } from "@nivo/pie";
import { tokens } from "../theme";
import { useTheme } from "@mui/material";
import { useState, useEffect } from "react";
import { Box, CircularProgress, Typography } from "@mui/material";

const PieChart = ({ isDashboard = false, cluster = null, enableFilter = false, chartType = "gender" }) => {
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
        let url;
        if (chartType === "service-usage") {
          url = "http://localhost:8000/api/charts/service-usage";
        } else {
          url = "http://localhost:8000/api/charts/gender-distribution";
        }

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

        // Transform data for Nivo pie chart format
        let transformedData;
        if (chartType === "service-usage") {
          // API returns: [{"service": "Grocery Stores, Supermarkets", "count": 2860738}]
          // Nivo expects: [{"id": "Grocery Stores, Supermarkets", "value": 2860738}]
          transformedData = jsonData.map((item) => ({
            id: item.service,
            label: item.service,
            value: item.count,
          }));
        } else {
          // API returns: [{"gender": "Female", "count": 1016}, {"gender": "Male", "count": 984}]
          // Nivo expects: [{"id": "Female", "value": 1016}, {"id": "Male", "value": 984}]
          transformedData = jsonData.map((item) => ({
            id: item.gender,
            label: item.gender,
            value: item.count,
          }));
        }

        setData(transformedData);
      } catch (err) {
        setError(err.message || "Failed to load chart data");
        console.error("Error fetching pie chart data:", err);
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
    <ResponsivePie
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
            background: colors.primary[400],
            color: colors.gray[100],
            fontSize: 12,
            borderRadius: 4,
            boxShadow: '0 3px 14px rgba(0, 0, 0, 0.4)',
          },
        },
      }}
      margin={{ top: 40, right: 80, bottom: 80, left: 80 }}
      innerRadius={0.5}
      padAngle={0.7}
      cornerRadius={3}
      activeOuterRadiusOffset={8}
      borderColor={{
        from: "color",
        modifiers: [["darker", 0.2]],
      }}
      arcLinkLabelsSkipAngle={10}
      arcLinkLabelsTextColor={colors.gray[100]}
      arcLinkLabelsThickness={2}
      arcLinkLabelsColor={{ from: "color" }}
      enableArcLabels={false}
      arcLabelsRadiusOffset={0.4}
      arcLabelsSkipAngle={7}
      arcLabelsTextColor={{
        from: "color",
        modifiers: [["darker", 2]],
      }}
      defs={[
        {
          id: "dots",
          type: "patternDots",
          background: "inherit",
          color: "rgba(255, 255, 255, 0.3)",
          size: 4,
          padding: 1,
          stagger: true,
        },
        {
          id: "lines",
          type: "patternLines",
          background: "inherit",
          color: "rgba(255, 255, 255, 0.3)",
          rotation: -45,
          lineWidth: 6,
          spacing: 10,
        },
      ]}
      legends={[]}
      tooltip={({ datum }) => {
        // Calculate percentage manually
        const total = data.reduce((sum, item) => sum + item.value, 0);
        const percentage = total > 0 ? ((datum.value / total) * 100).toFixed(1) : 0;

        return (
          <div
            style={{
              background: colors.primary[400],
              color: colors.gray[100],
              padding: '12px 16px',
              borderRadius: '4px',
              boxShadow: '0 3px 14px rgba(0, 0, 0, 0.4)',
              border: `1px solid ${colors.primary[300]}`,
            }}
          >
            <div style={{ fontWeight: 'bold', marginBottom: '4px' }}>
              {datum.label}
            </div>
            <div style={{ fontSize: '14px' }}>
              Count: <strong>{datum.value.toLocaleString()}</strong>
            </div>
            <div style={{ fontSize: '14px' }}>
              Percentage: <strong>{percentage}%</strong>
            </div>
          </div>
        );
      }}
    />
  );
};

export default PieChart;
