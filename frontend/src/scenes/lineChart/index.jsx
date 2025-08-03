import { Box } from "@mui/material";
import Header from "../../components/Header";
import LineChart from "../../components/LineChart";

const Line = () => {
  return (
    <Box m="20px">
      <Header title="Line Chart" subtitle="Simple Line Chart" />
      <Box height="75vh">
        <LineChart
          chartType="transactions-by-hour"
          isDashboard={false}
          enableFilter={false}
        />
      </Box>
    </Box>
  );
};

export default Line;