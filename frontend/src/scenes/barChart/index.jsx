import { Box, useTheme, useMediaQuery } from "@mui/material";
import { useState } from "react";
import Header from "../../components/Header";
import BarChart from "../../components/BarChart";

const Bar = () => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down("sm"));
  const [selectedCluster, setSelectedCluster] = useState(null);

  const barChartProps = isMobile
      ? {
        margin: { top: 50, right: 50, bottom: 100, left: 60 },
        legends: [
          {
            dataFrom: "keys",
            anchor: "bottom",
            direction: "row",
            justify: false,
            translateX: 20,
            translateY: 80,
            itemsSpacing: 2,
            itemWidth: 80,
            itemHeight: 20,
            itemDirection: "left-to-right",
            itemOpacity: 0.85,
            symbolSize: 18,
            effects: [
              {
                on: "hover",
                style: {
                  itemOpacity: 1,
                },
              },
            ],
          },
        ],
      }
      : {};

  return (
      <Box m="20px">
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Header title="Services Usage" subtitle="Overview of Usage by Service" />
        </Box>
        <Box height="75vh">
          <BarChart {...barChartProps}  />
        </Box>
      </Box>
  );
};

export default Bar;