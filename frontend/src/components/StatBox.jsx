import { Box, Typography, useTheme, CircularProgress } from "@mui/material";
import { tokens } from "../theme";
import TrendingUpIcon from "@mui/icons-material/TrendingUp";
import TrendingDownIcon from "@mui/icons-material/TrendingDown";
import { useState, useEffect } from "react";

const StatBox = ({
  title,
  subtitle,
  icon,
  trendline,
  increase,
  apiEndpoint = null,
  enableApi = false,
  dataType = "number" // "number", "currency", "service"
}) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  const [apiData, setApiData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (enableApi && apiEndpoint) {
      const fetchStatData = async () => {
        try {
          setIsLoading(true);
          const response = await fetch(`http://localhost:8000${apiEndpoint}`);

          if (!response.ok) {
            throw new Error(`API error: ${response.status}`);
          }

          const data = await response.json();
          setApiData(data);
        } catch (err) {
          setError(`${err.message} - Endpoint: ${apiEndpoint}`);
          console.error("Error fetching stat data:", err);
          console.error("Failed endpoint:", apiEndpoint);
          console.error("Full error details:", err);
        } finally {
          setIsLoading(false);
        }
      };

      fetchStatData();
    }
  }, [apiEndpoint, enableApi]);

  // Format data based on type and API response
  const getFormattedData = () => {
    if (!enableApi || !apiData) {
      return { title, subtitle, increase };
    }

    let formattedTitle = "";
    let formattedSubtitle = "";

    switch (dataType) {
      case "currency":
        if (apiData.average_clv !== undefined) {
          formattedTitle = `$${apiData.average_clv.toLocaleString()}`;
          formattedSubtitle = "Average CLV";
        }
        break;
      case "service":
        if (apiData.most_used_service !== undefined) {
          formattedTitle = apiData.most_used_service;
          formattedSubtitle = "Most Used Service";
        }
        break;
      default: // "number"
        if (apiData.new_users !== undefined) {
          formattedTitle = apiData.new_users.toLocaleString();
          formattedSubtitle = "New Users";
        } else if (apiData.returning_users !== undefined) {
          formattedTitle = apiData.returning_users.toLocaleString();
          formattedSubtitle = "Returning Users";
        }
        break;
    }

    return {
      title: formattedTitle || title,
      subtitle: formattedSubtitle || subtitle,
      increase: increase || "+12%" // Default trend
    };
  };

  const displayData = getFormattedData();

  // Determine trend icon and color
  const getTrendIcon = () => {
    if (apiData?.trend === 'up' || (!apiData && increase?.startsWith('+'))) {
      return <TrendingUpIcon sx={{ color: colors.greenAccent[600], fontSize: "26px" }} />;
    } else {
      return <TrendingDownIcon sx={{ color: colors.redAccent[500], fontSize: "26px" }} />;
    }
  };

  const getTrendColor = () => {
    if (apiData?.trend === 'up' || (!apiData && increase?.startsWith('+'))) {
      return colors.greenAccent[600];
    } else {
      return colors.redAccent[500];
    }
  };

  if (isLoading) {
    return (
      <Box width="100%" m="0 30px" display="flex" justifyContent="center" alignItems="center" height="100px">
        <CircularProgress size={30} />
      </Box>
    );
  }

  if (error) {
    return (
      <Box width="100%" m="0 30px">
        <Typography variant="h6" color="error" fontSize="12px">
          Error loading data
        </Typography>
      </Box>
    );
  }

  return (
    <Box width="100%" m="0 30px">
      <Box display="flex" justifyContent="space-between">
        <Box>
          {icon}
          <Typography
            variant="h4"
            fontWeight="bold"
            sx={{ color: colors.gray[100] }}
          >
            {displayData.title}
          </Typography>
        </Box>
        <Box>
          {trendline || getTrendIcon()}
        </Box>
      </Box>
      <Box display="flex" justifyContent="space-between" mt="2px">
        <Typography variant="h5" sx={{ color: colors.greenAccent[500] }}>
          {displayData.subtitle}
        </Typography>
        <Typography
          variant="h5"
          fontStyle="italic"
          sx={{ color: getTrendColor() }}
        >
          {displayData.increase}
        </Typography>
      </Box>
      {apiData?.period && (
        <Typography variant="body2" sx={{ color: colors.gray[300], fontSize: "10px", mt: "5px" }}>
          {apiData.period}
        </Typography>
      )}
    </Box>
  );
};

export default StatBox;