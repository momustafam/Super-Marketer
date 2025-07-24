import { Box, Typography, useTheme } from "@mui/material";
import { tokens } from "../theme";
import React from 'react';

const StatBox = ({ title, subtitle, icon, trendline, increase }) => {
  const theme = useTheme();
  const colors = tokens(theme.palette.mode);

  // Convert string to number and check if it's negative
  const isNegative = typeof increase === "string" && increase.trim().startsWith("-");

  // Decide color based on whether the increase is negative
  const increaseColor = isNegative ? colors.redAccent[500] : colors.greenAccent[600];

  // Clone the trendline icon with new color if negative
  const coloredTrendline = trendline && React.cloneElement(trendline, {
    sx: { color: increaseColor, fontSize: "26px" },
  });

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
            {title}
          </Typography>
        </Box>
        <Box>
          {coloredTrendline}
        </Box>
      </Box>
      <Box display="flex" justifyContent="space-between" mt="2px">
        <Typography variant="h5" sx={{ color: colors.greenAccent[500] }}>
          {subtitle}
        </Typography>
        <Typography
          variant="h5"
          fontStyle="italic"
          sx={{ color: increaseColor }}
        >
          {increase}
        </Typography>
      </Box>
    </Box>
  );
};

export default StatBox;