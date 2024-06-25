import { useState, useEffect } from "react";
import axios from "axios";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

interface OrderStatusData {
  order_item_status: string;
  count: number;
}

const OrderStatusChart: React.FC = () => {
  const [data, setData] = useState<OrderStatusData[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get<OrderStatusData[]>(
          "https://moneyflo-backend.onrender.com"
        );
        setData(response.data);
      } catch (error) {
        console.log("Error fetching data", error);
      }
    };
    fetchData();
  }, []);

  const chartData = {
    labels: data.map((item) => item.order_item_status),
    datasets: [
      {
        label: "Number of Orders",
        data: data.map((item) => item.count),
        backgroundColor: "rgba(75, 192, 192, 0.2)",
        borderColor: "rgba(75, 192, 192, 1)",
        borderWidth: 1,
      },
    ],
  };

  return (
    <div>
      <h2>Order Status Distribution</h2>

      <Bar
        data={chartData}
        options={{
          responsive: true,
          plugins: {
            legend: {
              position: "top",
            },
            title: {
              display: true,
              text: "Order Status Distribution",
            },
          },
        }}
      />
    </div>
  );
};

export default OrderStatusChart;
