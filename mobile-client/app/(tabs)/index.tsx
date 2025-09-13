import { View, Text, StyleSheet, FlatList, TouchableOpacity, Button } from 'react-native';

import { ThemedView } from '@/components/themed-view';
import { ThemedText } from '@/components/themed-text';
import { schedulePushNotification } from '../_layout';

export default function HomeScreen() {
  const transactions = [
    { id: '1', title: 'Grocery Store', amount: '-$45.00', date: 'Sep 10' },
    { id: '2', title: 'Salary', amount: '+$1500.00', date: 'Sep 9' },
    { id: '3', title: 'Coffee Shop', amount: '-$5.50', date: 'Sep 8' },
  ];

  return (
    <ThemedView style={styles.container}>
      {/* Account Balance Section */}
      <View style={styles.balanceContainer}>
        <ThemedText type="title" style={styles.balanceTitle}>
          Account Balance
        </ThemedText>
        <ThemedText type="title" style={styles.balanceAmount}>
          $12,345.67
        </ThemedText>
      </View>

      {/* Quick Actions Section */}
      <View style={styles.quickActionsContainer}>
        <TouchableOpacity style={styles.actionButton}>
          <Text style={styles.actionText}>Send Money</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.actionButton}>
          <Text style={styles.actionText}>Pay Bills</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.actionButton}>
          <Text style={styles.actionText}>Request Money</Text>
        </TouchableOpacity>
      </View>

      {/* Recent Transactions Section */}
      <View style={styles.transactionsContainer}>
        <ThemedText type="subtitle" style={styles.sectionTitle}>
          Recent Transactions
        </ThemedText>
        <FlatList
          data={transactions}
          keyExtractor={(item) => item.id}
          renderItem={({ item }) => (
            <View style={styles.transactionItem}>
              <Text style={styles.transactionTitle}>{item.title}</Text>
              <Text style={styles.transactionAmount}>{item.amount}</Text>
              <Text style={styles.transactionDate}>{item.date}</Text>
            </View>
          )}
        />
      </View>
      <Button
        title="Press to schedule a notification"
        onPress={async () => {
          await schedulePushNotification();
        }}
      />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 16,
  },
  balanceContainer: {
    marginBottom: 24,
    alignItems: 'center',
  },
  balanceTitle: {
    fontSize: 18,
    marginBottom: 8,
  },
  balanceAmount: {
    fontSize: 32,
    fontWeight: 'bold',
  },
  quickActionsContainer: {
    flexDirection: 'row',
    justifyContent: 'space-around',
    marginBottom: 24,
  },
  actionButton: {
    backgroundColor: '#4CAF50',
    padding: 12,
    borderRadius: 8,
  },
  actionText: {
    color: '#fff',
    fontWeight: 'bold',
  },
  transactionsContainer: {
    flex: 1,
  },
  sectionTitle: {
    marginBottom: 16,
  },
  transactionItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    padding: 12,
    borderBottomWidth: 1,
    borderBottomColor: '#ccc',
  },
  transactionTitle: {
    fontSize: 16,
  },
  transactionAmount: {
    fontSize: 16,
    fontWeight: 'bold',
  },
  transactionDate: {
    fontSize: 14,
    color: '#888',
  },
});
