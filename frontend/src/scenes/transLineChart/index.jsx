import { Box } from "@mui/material";
import Header from "../../components/Header";
import LineChart from "../../components/LineChart";

const TransLine = () => {
  return (
    <Box m="20px">
      <Header title="Transaction Analysis" subtitle="Transaction Patterns and Trends" />
      <Box height="75vh">
        <LineChart
          chartType="revenue-trends"
          isDashboard={false}
          enableFilter={false}
        />
      </Box>
    </Box>
  );
};

export default TransLine;
