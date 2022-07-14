create sequence hibernate_sequence start 1 increment 1;
create table account (id int4 not null, amount int4 not null, user_id int4, primary key (id));
create table users (id int4 not null, user_name varchar(255), primary key (id));
alter table if exists users add constraint UK_k8d0f2n7n88w1a16yhua64onx unique (user_name);
alter table if exists account add constraint FKra7xoi9wtlcq07tmoxxe5jrh4 foreign key (user_id) references users;
