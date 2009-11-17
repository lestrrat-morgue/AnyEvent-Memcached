package AnyEvent::Memcached::Protocol::Text;
use Any::Moose;
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Protocol';

{
    my $generator = sub {
        my $cmd = shift;
        return sub {
            my ($self, $memcached, $key, $value, $cb) = @_;
            my $handle = $self->get_handle_for( $key );
        
            $value ||= 1;
            my @command = ($cmd => $key => $value);
            my $noreply = 0; # XXX - FIXME
            if ($noreply) {
                push @command, "noreply";
            }
            $handle->push_write(join(' ', @command) . "\r\n");

            if (! $noreply) {
                $handle->push_read(regex => qr{\r\n}, sub {
                    my $data = $_[1];
                    $data =~ /^(NOT_FOUND|\w+)\r\n/;
                    $cb->($1 eq 'NOT_FOUND' ? undef : $1) if $cb;
                    $memcached->drain_queue;
                });
            }
        }
    };

    sub _build_decr_cb {
        my $self = shift;
        $generator->("decr");
    }

    sub _build_incr_cb {
        my $self = shift;
        $generator->("incr");
    }
}

sub _build_delete_cb {
    return sub {
        my ($self, $memcached, $key, $noreply, $cb) = @_;
        my $handle = $self->get_handle_for( $key );

        my @command = (delete => $key);
        $noreply = 0; # XXX - FIXME
        if ($noreply) {
            push @command, "noreply";
        }
        $handle->push_write(join(' ', @command) . "\r\n");

        if (! $noreply) {
            $handle->push_read(regex => qr{\r\n}, sub {
                my $data = $_[1];
                my $success = $data =~ /^DELETED\r\n/;
                $cb->($success) if $cb;
                $memcached->drain_queue;
            });
        }
    };
}

sub _build_get_multi_cb {
    return sub {
        my ($self, $memcached, $keys, $cb, $cb_caller) = @_;
        my %rv;
        my $cv = AE::cv {
            $cb_caller->($cb, \%rv);
            $memcached->drain_queue;
        };

        if (scalar @$keys == 0) {
            $cv->send;
            return;
        }

        my $count = $memcached->get_server_count();
        my @keysinserver;
        foreach my $key (@$keys) {
            my $hash   = $memcached->hashing_algorithm->hash($key);
            my $i      = $hash % $count;
            my $handle = $memcached->get_handle( $memcached->get_server($i) );
            my $list = $keysinserver[ $i ];
            if (! $list) {
                $keysinserver[ $i ] = $list = [ $handle ];
            }
            push @$list, $key;
        }
   
        for my $i (0..$#keysinserver) {
            next unless $keysinserver[$i];
            my ($handle, @keylist) = @{$keysinserver[$i]};
            $cv->begin;
            $handle->push_write( "get @keylist\r\n" );
            my $code; $code = sub {
                my ($handle, $line) = @_;
                if ($line =~ /^END(?:\r\n)?$/) {
                    undef $code;
                    $cv->end;
                } elsif ($line =~ /^VALUE (\S+) (\S+) (\S+)(?: (\S+))?/)  {
                    my ($rkey, $rflags, $rsize, $rcas) = ($1, $2, $3, $4);
                    $handle->push_read(chunk => $rsize, sub {
                        my $data = $self->decode_value($rflags, $_[1]);

                        $rv{ $rkey } = $data; # XXX whatabout CAS?
                        $handle->push_read(regex => qr{\r\n}, cb => sub { "noop" });
                        $handle->push_read(line => $code);
                    } );
                } else {
                    confess("Unexpected line $line");
                }
            };
            $handle->push_read(line => $code);
        }
    };
}

{
    my $generator = sub {
        my $cmd = shift;
        sub {
            my ($self, $memcached, $key, $value, $exptime, $noreply, $cb) = @_;
            my $handle = $self->get_handle_for( $key );

            my ($write_data, $write_len, $flags, $expires) =
                $self->prepare_value( $cmd, $value, $exptime );
            $handle->push_write("$cmd $key $flags $expires $write_len\r\n$write_data\r\n");
            $handle->push_read(regex => qr{^STORED\r\n}, sub {
                $cb->(1) if $cb;
                $self->memcached->drain_queue;
            });
        };
    };

    sub _build_add_cb {
        my $self = shift;
        return $generator->( "add" );
    }

    sub _build_replace_cb {
        my $self = shift;
        return $generator->( "replace" );
    }

    sub _build_set_cb {
        my $self = shift;
        return $generator->( "set" );
    }
}

sub _build_stats_cb {
    return sub {
        my ($self, $memcached, $name, $cb) = @_;

        my %rv;
        my $cv = AE::cv {
            $cb->( \%rv );
        };

        foreach my $server ($memcached->all_servers) {
            my $handle = $memcached->get_handle( $server );

            $cv->begin;
            $handle->push_write( $name ? "stats $name\r\n" : "stats\r\n" );
            my $code; $code = sub {
                my ($handle, $line) = @_;
                if ($line eq 'END') {
                    $cv->end;
                } elsif ( $line =~ /^STAT (\S+) (\S+)$/) {
                    $rv{ $server }->{ $1 } = $2;
                    $handle->push_read( line => $code );
                }
            };
            $handle->push_read( line => $code );
        }
    }
}

__PACKAGE__->meta->make_immutable();

1;

__END__

=head1 NAME

AnyEvent::Memcached::Protocol::Text - Implements Memcached Text Protocol

=cut
