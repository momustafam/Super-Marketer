import { Box } from "@mui/material";
import Header from "../../components/Header";
import LineChart from "../../components/LineChart";

const TransLine = () => {
  return (
    <Box m="20px">
      <Header title="Transactions by Day" subtitle="Transaction Volume Over Time" />
      <Box height="75vh">
        <LineChart />
      </Box>
    </Box>
  );
};

export default TransLine;