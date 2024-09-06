package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class Main {
    static Connection connection = null;
    static Statement statement = null;

    public static void connect() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "27lopare";

        try {
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void disconnect() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        connect();

        statement.execute("DROP TABLE IF EXISTS public.progress;");
        statement.execute("DROP TABLE IF EXISTS public.chefs;");
        statement.execute("DROP TABLE IF EXISTS public.menu;");

        // Создание таблицы поваров
        statement.execute("CREATE TABLE IF NOT EXISTS chefs " +
                "(id SERIAL NOT NULL PRIMARY KEY, " +
                "name VARCHAR(30) NOT NULL, " +
                "PassportSerial VARCHAR(4) NOT NULL, " +
                "PassportNumber VARCHAR(6) NOT NULL, " +
                "UNIQUE (PassportSerial, PassportNumber));");

        // Создание таблицы меню
        statement.execute("CREATE TABLE IF NOT EXISTS menu " +
                "(id SERIAL NOT NULL PRIMARY KEY, " +
                "name VARCHAR(50) NOT NULL);");

        // Создание таблицы выработки с каскадным удалением
        statement.execute("CREATE TABLE IF NOT EXISTS progress " +
                "(id SERIAL NOT NULL PRIMARY KEY, " +
                "chef INT NOT NULL REFERENCES chefs(id) ON DELETE CASCADE, " + // Каскадное удаление при удалении повара
                "menu INT NOT NULL REFERENCES menu(id), " +
                "orders INT NOT NULL CHECK(orders >= 0));");

        // Вставка данных в таблицу поваров
        statement.execute("INSERT INTO chefs (id, name, PassportSerial, PassportNumber) " +
                "VALUES " +
                "(1, 'Алексей', '3311', '435678'), " +
                "(2, 'Сергей', '3322', '342345'), " +
                "(3, 'Данил', '3333', '348945'), " +
                "(4, 'Роман', '3344', '234567'), " +
                "(5, 'Артем', '3355', '345678'), " +
                "(6, 'Тихон', '3300', '890123');");

        // Вставка данных в таблицу меню
        statement.execute("INSERT INTO menu (id, name) " +
                "VALUES " +
                "(3, 'Бизнес'), " +
                "(2, 'Взрослое'), " +
                "(1, 'Детское'), " +
                "(4, 'Корпоратив');");

        // Вставка данных в таблицу выработки
        statement.execute("INSERT INTO progress (id, chef, menu, orders) " +
                "VALUES " +
                "(1, 1, 3, 70), " + // Алексей - Бизнес
                "(2, 2, 1, 60), " + // Сергей - Детское
                "(3, 3, 2, 80), " + // Данил - Взрослое
                "(4, 4, 4, 85), " + // Роман - Корпоратив
                "(5, 5, 3, 90), " + // Артем - Бизнес
                "(6, 6, 2, 95), " + // Тихон - Взрослое
                "(7, 1, 1, 25), " + // Алексей - Бизнес
                "(8, 2, 2, 34), " + // Сергей - Детское
                "(9, 3, 3, 51), " + // Данил - Взрослое
                "(10,4, 1, 43), " + // Роман - Корпоратив
                "(11, 5, 2, 65)," + // Артем - Бизнес
                "(12, 6, 1, 47);");  // Тихон - Взрослое

       // Удаление повара с id = 1
       //statement.execute("DELETE FROM chefs WHERE id = 1;");
        var res = statement.executeQuery("SELECT c.name, p.orders, m.name FROM chefs c " +
                "INNER JOIN progress p ON c.id = p.chef " +
                "INNER JOIN menu m ON p.menu = m.id " +
                "ORDER BY p.orders DESC " +
                "LIMIT 4 OFFSET 2;");

        while (res.next()) { // Обработка результатов запроса
            String chefName = res.getString(1); // Получение имени повара
            int orders = res.getInt(2); // Получение количества заказов
            String menuName = res.getString(3); // Получение названия меню
            System.out.println(chefName + " " + orders + " " + menuName); // Вывод информации о поваре, заказах и меню
        }

                //"WHERE p.orders = (SELECT MAX(p2.orders) FROM progress p2 WHERE p2.chef = c.id) " +
                //"AND p.orders > 50 "+



// Подсчет среднего количества заказов по меню
        System.out.println("Посчитать среднее количество заказов по определенному меню");
        var res2 = statement.executeQuery("SELECT AVG(p.orders) AS Среднее_количество FROM progress p " + // Выполнение SQL-запроса  заказовдля подсчета среднего количества
                "INNER JOIN menu m ON p.menu = m.id " // Соединение таблиц progress и menu по id меню
                );

        while (res2.next()) { // Обработка результатов запроса
            double avg = res2.getDouble(1); // Получение среднего количества заказов
            System.out.println(avg); // Вывод среднего количества заказов
        }

// Подсчет среднего количества заказов по определенному повару
        System.out.println("Посчитать среднее количество заказов по определенному повару");
        var res3 = statement.executeQuery("SELECT AVG(p.orders) AS Среднее_количество FROM progress p " + // Выполнение SQL-запроса для подсчета среднего количества заказов
                "INNER JOIN chefs c ON p.chef = c.id " + // Соединение таблиц progress и chefs по id повара
                "WHERE c.name = 'Алексей';"); // Условие: имя повара "Алексей"

        while (res3.next()) { // Обработка результатов запроса
            double avg = res3.getDouble(1); // Получение среднего количества заказов
            System.out.println(avg); // Вывод среднего количества заказов
        }

// Поиск трех меню, которые выполнили наибольшее количество заказов
        System.out.println("Найти три меню, которые выполнили наибольшее количество заказов");
        var res4 = statement.executeQuery("SELECT SUM(p.orders) AS total_orders, m.name FROM progress p " + // Выполнение SQL-запроса для поиска трех меню
                "INNER JOIN menu m ON m.id = p.menu " + // Соединение таблиц progress и menu по id меню
                "GROUP BY m.name " + // Группировка по названию меню
                "ORDER BY total_orders DESC LIMIT 3;"); // Условие: ограничение до 3 меню с наибольшим количеством заказов

        while (res4.next()) { // Обработка результатов запроса
            int totalOrders = res4.getInt(1); // Получение общего количества заказов
            String menuName = res4.getString(2); // Получение названия меню
            System.out.println(totalOrders + " " + menuName); // Вывод общего количества заказов и названия меню
        }
        disconnect();
    }
}